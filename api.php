<?php
\Bitrix\Main\Loader::includeModule("crm");
use \Bitrix\Main,
	\Bitrix\Crm\Service;
require_once (__DIR__.'/lib/app/crest.php');

class DealRelationManager {
    private $entityManager;
    private $logger;
    
    public function __construct(EntityManager $entityManager, JsonLogger $logger = null) {
        $this->entityManager = $entityManager;
        $this->logger = $logger ?: new JsonLogger();
    }
    
    /**
     * Находит и привязывает связанные элементы к сделке
     */
    public function findAndAttachRelationsToDeal($dealId, $dealFields) {
        $relations = [];
        
        // 1. Находим склад (смарт-процесс ID 1044) по warehouse_code
        if (!empty($dealFields['UF_CRM_1756713651'])) { // warehouse_code
            $warehouseId = $this->findWarehouseByCode($dealFields['UF_CRM_1756713651']);
            if ($warehouseId) {
                $relations['warehouse'] = $warehouseId;
                $this->attachWarehouseToDeal($dealId, $warehouseId);
            }
        }
        
        // 2. Находим карту (смарт-процесс ID 1038) по card_number
        if (!empty($dealFields['UF_CRM_1756712343'])) { // card_number
            $cardId = $this->findCardByNumber($dealFields['UF_CRM_1756712343']);
            if ($cardId) {
                $relations['card'] = $cardId;
                $this->attachCardToDeal($dealId, $cardId);
                
                // 3. Привязываем клиента из карты к сделке
                $clientId = $this->getClientFromCard($cardId);
                if ($clientId) {
                    $relations['client'] = $clientId;
                    $this->attachClientToDeal($dealId, $clientId);
                }
            }
        }
        
        return $relations;
    }
    
    /**
     * Находит склад по коду (warehouse_code)
     */
    private function findWarehouseByCode($warehouseCode) {
        try {
            $factory = Service\Container::getInstance()->getFactory(1044);
            if (!$factory) {
                error_log("Фабрика для смарт-процесса складов (1044) не найдена");
                return null;
            }
            
            $items = $factory->getItems([
                'filter' => [
                    '=UF_CRM_4_CODE' => $warehouseCode
                ],
                'limit' => 1
            ]);
            
            if (!empty($items)) {
                $warehouse = $items[0];
                return $warehouse->getId();
            }
            
            $this->logger->logMappingError('deal', 'unknown', 'warehouse_code', $warehouseCode, [
                'warehouse_code' => $warehouseCode,
                'message' => 'Склад не найден по коду'
            ]);
            
            return null;
            
        } catch (Exception $e) {
            error_log("Ошибка при поиске склада: " . $e->getMessage());
            $this->logger->logGeneralError('deal', 'unknown', "Ошибка поиска склада: " . $e->getMessage(), [
                'warehouse_code' => $warehouseCode
            ]);
            return null;
        }
    }
    
    /**
     * Находит карту по номеру (card_number)
     */
    private function findCardByNumber($cardNumber) {
        try {
            $factory = Service\Container::getInstance()->getFactory(1038);
            if (!$factory) {
                error_log("Фабрика для смарт-процесса карт (1038) не найдена");
                return null;
            }
            
            $items = $factory->getItems([
                'filter' => [
                    '=UF_CRM_3_1759320971349' => $cardNumber // number поле карты
                ],
                'limit' => 1
            ]);
            
            if (!empty($items)) {
                $card = $items[0];
                return $card->getId();
            }
            
            $this->logger->logMappingError('deal', 'unknown', 'card_number', $cardNumber, [
                'card_number' => $cardNumber,
                'message' => 'Карта не найдена по номеру'
            ]);
            
            return null;
            
        } catch (Exception $e) {
            error_log("Ошибка при поиске карты: " . $e->getMessage());
            $this->logger->logGeneralError('deal', 'unknown', "Ошибка поиска карты: " . $e->getMessage(), [
                'card_number' => $cardNumber
            ]);
            return null;
        }
    }
    
    /**
     * Получает клиента из карты
     */
    private function getClientFromCard($cardId) {
        try {
            $factory = Service\Container::getInstance()->getFactory(1038);
            if (!$factory) {
                return null;
            }
            
            $card = $factory->getItem($cardId);
            if (!$card) {
                return null;
            }
            
            // Получаем поле клиента из карты (предполагаемое поле UF_CRM_3_CLIENT)
            $clientField = $card->get('UF_CRM_3_CLIENT');
            if (!empty($clientField)) {
                return $clientField;
            }
            
            return null;
            
        } catch (Exception $e) {
            error_log("Ошибка при получении клиента из карты: " . $e->getMessage());
            return null;
        }
    }
    
    /**
     * Привязывает склад к сделке
     */
    private function attachWarehouseToDeal($dealId, $warehouseId) {
        try {
            $deal = new \CCrmDeal(false);
            $updateFields = [
                'UF_CRM_1756713651' => $warehouseId // Предполагаемое поле для связи со складом
            ];
            
            $result = $deal->Update($dealId, $updateFields);
            
            if ($result) {
                $this->logger->logSuccess('deal_relation', $dealId, "Склад привязан: {$warehouseId}", [
                    'deal_id' => $dealId,
                    'warehouse_id' => $warehouseId,
                    'relation_type' => 'warehouse'
                ]);
            } else {
                $this->logger->logGeneralError('deal_relation', $dealId, "Ошибка привязки склада", [
                    'deal_id' => $dealId,
                    'warehouse_id' => $warehouseId
                ]);
            }
            
            return $result;
            
        } catch (Exception $e) {
            $this->logger->logGeneralError('deal_relation', $dealId, "Ошибка привязки склада: " . $e->getMessage(), [
                'deal_id' => $dealId,
                'warehouse_id' => $warehouseId
            ]);
            return false;
        }
    }
    
    /**
     * Привязывает карту к сделке
     */
    private function attachCardToDeal($dealId, $cardId) {
        try {
            $deal = new \CCrmDeal(false);
            $updateFields = [
                'UF_CRM_1756712343' => $cardId // Предполагаемое поле для связи с картой
            ];
            
            $result = $deal->Update($dealId, $updateFields);
            
            if ($result) {
                $this->logger->logSuccess('deal_relation', $dealId, "Карта привязана: {$cardId}", [
                    'deal_id' => $dealId,
                    'card_id' => $cardId,
                    'relation_type' => 'card'
                ]);
            } else {
                $this->logger->logGeneralError('deal_relation', $dealId, "Ошибка привязки карты", [
                    'deal_id' => $dealId,
                    'card_id' => $cardId
                ]);
            }
            
            return $result;
            
        } catch (Exception $e) {
            $this->logger->logGeneralError('deal_relation', $dealId, "Ошибка привязки карты: " . $e->getMessage(), [
                'deal_id' => $dealId,
                'card_id' => $cardId
            ]);
            return false;
        }
    }
    
    /**
     * Привязывает клиента к сделке
     */
    private function attachClientToDeal($dealId, $clientId) {
        try {
            $deal = new \CCrmDeal(false);
            $updateFields = [
                'CONTACT_ID' => $clientId // Стандартное поле для связи с контактом
            ];
            
            $result = $deal->Update($dealId, $updateFields);
            
            if ($result) {
                $this->logger->logSuccess('deal_relation', $dealId, "Клиент привязан: {$clientId}", [
                    'deal_id' => $dealId,
                    'client_id' => $clientId,
                    'relation_type' => 'client'
                ]);
            } else {
                $this->logger->logGeneralError('deal_relation', $dealId, "Ошибка привязки клиента", [
                    'deal_id' => $dealId,
                    'client_id' => $clientId
                ]);
            }
            
            return $result;
            
        } catch (Exception $e) {
            $this->logger->logGeneralError('deal_relation', $dealId, "Ошибка привязки клиента: " . $e->getMessage(), [
                'deal_id' => $dealId,
                'client_id' => $clientId
            ]);
            return false;
        }
    }
}
class JsonLogger {
    private $logFile;
    private $logData;
    
    public function __construct($logFile = __DIR__.'/import_log.json') {
        $this->logFile = $logFile;
        $this->logData = [];
        $this->loadExistingLog();
    }
    
    /**
     * Загружает существующий лог файл
     */
    private function loadExistingLog() {
        if (file_exists($this->logFile)) {
            $content = file_get_contents($this->logFile);
            $this->logData = json_decode($content, true) ?: [];
        }
    }
    
    /**
     * Сохраняет лог в файл
     */
    private function saveLog() {
        \Bitrix\Main\Diag\Debug::writeToFile(json_encode($this->logData, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE));
    }
    
    /**
     * Логирует пустое поле
     */
    public function logEmptyField($entityType, $entityId, $fieldName, $fieldValue = null, $itemData = []) {
        $this->logData[] = [
            'timestamp' => date('Y-m-d H:i:s'),
            'type' => 'empty_field',
            'entity_type' => $entityType,
            'entity_id' => $entityId,
            'field_name' => $fieldName,
            'field_value' => $fieldValue,
            'message' => "Пустое поле: {$fieldName}",
            'item_data' => $itemData
        ];
        $this->saveLog();
    }
    
    /**
     * Логирует несколько пустых полей сразу
     */
    public function logMultipleEmptyFields($entityType, $entityId, $emptyFields, $itemData = []) {
        foreach ($emptyFields as $fieldName => $fieldValue) {
            $this->logEmptyField($entityType, $entityId, $fieldName, $fieldValue, $itemData);
        }
    }
    
    /**
     * Логирует ошибку сопоставления по коду/ID
     */
    public function logMappingError($entityType, $entityId, $codeField, $codeValue, $itemData = []) {
        $this->logData[] = [
            'timestamp' => date('Y-m-d H:i:s'),
            'type' => 'mapping_error',
            'entity_type' => $entityType,
            'entity_id' => $entityId,
            'code_field' => $codeField,
            'code_value' => $codeValue,
            'message' => "Не удалось сопоставить по {$codeField}: {$codeValue}",
            'item_data' => $itemData
        ];
        $this->saveLog();
    }
    
    /**
     * Логирует ошибку загрузки фото
     */
    public function logPhotoError($entityType, $entityId, $photoUrl, $error, $itemData = []) {
        $this->logData[] = [
            'timestamp' => date('Y-m-d H:i:s'),
            'type' => 'photo_error',
            'entity_type' => $entityType,
            'entity_id' => $entityId,
            'photo_url' => $photoUrl,
            'error' => $error,
            'message' => "Ошибка загрузки фото: {$photoUrl} - {$error}",
            'item_data' => $itemData
        ];
        $this->saveLog();
    }
    
    /**
     * Логирует отсутствие пользователя на портале
     */
    public function logUserNotFound($entityType, $entityId, $cashierCode, $itemData = []) {
        $this->logData[] = [
            'timestamp' => date('Y-m-d H:i:s'),
            'type' => 'user_not_found',
            'entity_type' => $entityType,
            'entity_id' => $entityId,
            'cashier_code' => $cashierCode,
            'message' => "Пользователь не найден на портале: {$cashierCode}",
            'item_data' => $itemData
        ];
        $this->saveLog();
    }
    
    /**
     * Логирует общую ошибку
     */
    public function logGeneralError($entityType, $entityId, $error, $itemData = []) {
        $this->logData[] = [
            'timestamp' => date('Y-m-d H:i:s'),
            'type' => 'general_error',
            'entity_type' => $entityType,
            'entity_id' => $entityId,
            'error' => $error,
            'message' => "Общая ошибка: {$error}",
            'item_data' => $itemData
        ];
        $this->saveLog();
    }
    
    /**
     * Логирует успешное создание
     */
    public function logSuccess($entityType, $entityId, $bitrixId, $itemData = []) {
        $this->logData[] = [
            'timestamp' => date('Y-m-d H:i:s'),
            'type' => 'success',
            'entity_type' => $entityType,
            'entity_id' => $entityId,
            'bitrix_id' => $bitrixId,
            'message' => "Успешно создано. Bitrix ID: {$bitrixId}",
            'item_data' => $itemData
        ];
        $this->saveLog();
    }
    
    /**
     * Получает статистику по логам
     */
    public function getStats() {
        $stats = [
            'total' => count($this->logData),
            'success' => 0,
            'empty_field' => 0,
            'mapping_error' => 0,
            'photo_error' => 0,
            'user_not_found' => 0,
            'general_error' => 0
        ];
        
        foreach ($this->logData as $log) {
            if (isset($stats[$log['type']])) {
                $stats[$log['type']]++;
            }
        }
        
        return $stats;
    }
    
    /**
     * Очищает лог файл
     */
    public function clearLog() {
        $this->logData = [];
        $this->saveLog();
    }

    /**
     * Проверяет успешность создания лога
     */
    public function isLogCreated() {
        return file_exists($this->logFile) && filesize($this->logFile) > 0;
    }
    
    /**
     * Получает информацию о лог файле
     */
    public function getLogInfo() {
        if (!file_exists($this->logFile)) {
            return [
                'success' => false,
                'message' => 'Лог файл не существует',
                'file_size' => 0,
                'entries_count' => 0
            ];
        }
        
        $fileSize = filesize($this->logFile);
        $entriesCount = count($this->logData);
        
        return [
            'success' => $fileSize > 0,
            'message' => $fileSize > 0 ? 'Лог успешно создан' : 'Лог файл пустой',
            'file_size' => $fileSize,
            'entries_count' => $entriesCount,
            'file_path' => realpath($this->logFile)
        ];
    }
}

class BatchProductManager {
    private $entityManager;
    private $batchSize;
    private $logger;
    
    public function __construct(EntityManager $entityManager, $batchSize = 50, JsonLogger $logger = null) {
        $this->entityManager = $entityManager;
        $this->batchSize = $batchSize;
        $this->logger = $logger ?: new JsonLogger();
    }
    
    /**
     * Создает товары пакетными запросами с обработкой брендов
     */
    public function createProductsBatch($productsData) {
        $allResults = [];
        $chunks = array_chunk($productsData, $this->batchSize);
        
        foreach ($chunks as $chunkIndex => $chunk) {
            echo "Обрабатываю batch " . ($chunkIndex + 1) . " из " . count($chunks) . " (товаров: " . count($chunk) . ")\n";
            
            // Сначала обрабатываем бренды для этого batch
            $this->processBrandsForBatch($chunk);
            
            // Создаем batch запрос для товаров
            $batch = $this->createProductsBatchRequest($chunk);
            
            // Выполняем batch запрос
            $batchResult = CRest::callBatch($batch);
            
            if (isset($batchResult['result']) && is_array($batchResult['result'])) {
                $allResults = array_merge($allResults, $batchResult['result']);
            } else {
                echo "Ошибка при обработке batch: ";
                if (isset($batchResult['error'])) {
                    echo $batchResult['error'] . "\n";
                    $this->logger->logGeneralError('batch', 'batch_'.$chunkIndex, $batchResult['error']);
                }
                if (isset($batchResult['error_description'])) {
                    echo $batchResult['error_description'] . "\n";
                }
                
                // Логируем ошибки для каждого товара в batch
                foreach ($chunk as $item) {
                    $entityId = $item["code"] ?? 'unknown';
                    $this->logger->logGeneralError('item', $entityId, "Batch processing failed", $item);
                }
            }
            
            // Пауза между запросами
            usleep(500000);
        }
        
        return $allResults;
    }
    
    /**
     * Обрабатывает бренды для batch товаров
     */
    private function processBrandsForBatch($productsChunk) {
        $brandsToProcess = [];
        
        // Собираем уникальные бренды из batch
        foreach ($productsChunk as $product) {
            if (!empty($product["brand_code"])) {
                $brandCode = $product["brand_code"];
                $brandName = $product["brand_name"] ?? $brandCode;
                
                if (!isset($brandsToProcess[$brandCode])) {
                    $brandsToProcess[$brandCode] = [
                        'name' => $brandName,
                        'code' => $brandCode
                    ];
                }
            }
        }
        
        // Создаем бренды которые еще не существуют
        foreach ($brandsToProcess as $brandData) {
            $this->entityManager->addEntity('brand', $brandData);
        }
    }
    
    /**
     * Создает batch запрос для товаров
     */
    private function createProductsBatchRequest($productsChunk) {
        $batch = [];
        
        foreach ($productsChunk as $index => $product) {
            // Получаем поля товара через существующий метод
            $productFields = $this->prepareProductFields($product);
            
            $batch["cmd_{$index}"] = [
                'method' => 'catalog.product.add',
                'params' => [
                    'fields' => $productFields
                ],
            ];
        }
        
        return $batch;
    }
    
    /**
     * Подготавливает поля товара для batch запроса
     */
    private function prepareProductFields($item) {
        // Получаем ID бренда если он существует
        $brandId = null;
        if (!empty($item["brand_code"])) {
            $brandId = $this->findBrandIdByCode($item["brand_code"]);
        }
        
        // Обрабатываем изображения
        $detailPicture = null;
        if (!empty($item["product_image_filename"])) {
            $detailPicture = $this->entityManager->processItemImage(
                "" . $item["product_image_filename"],
                $item["code"] ?? 'unknown'
            );
        }
        
        $nimPhoto1 = null;
        if (!empty($item["nim_photo1"])) {
            $nimPhoto1 = $this->entityManager->processItemImage(
                "" . $item["nim_photo1"],
                $item["code"] ?? 'unknown'
            );
        }
        
        // Формируем поля товара
        $fields = [
            'property64' => $item["code"] ?? '',
            'property65' => $item["name"] ?? '',
            'property66' => $item["uin"] ?? '',
            'name' => $item["product_name"] ?? 'Товар без названия',
            'property67' => $item["product_sku"] ?? '',
            'detailPicture' => $detailPicture ?? $nimPhoto1 ?? '',
            'property68' => $item["nim_photo1"] ?? '',
            'property79' => $item["product_image_filename"] ?? '',
            'property78' => $item["brand_code"] ?? '',
            'property70' => $item["size_name"] ?? '',
            'property71' => $item["metal_name"] ?? '',
            'property72' => $item["fineness_name"] ?? '',
            'property73' => $item["feature_name"] ?? '',
            'iblockId' => 14,
            'iblockSectionId' => 13,
            'active' => 'Y'
        ];
        
        // Добавляем ID бренда если найден
        if ($brandId) {
            $fields['property77'] = $brandId; // Предполагаемое поле для связи с брендом
        }
        
        return $fields;
    }
    
    /**
     * Ищет ID бренда по коду
     */
    private function findBrandIdByCode($brandCode) {
        try {
            $factory = Service\Container::getInstance()->getFactory(1052);
            if (!$factory) {
                return null;
            }
            
            $items = $factory->getItems([
                'filter' => [
                    '=UF_CRM_6_CODE' => $brandCode
                ],
                'limit' => 1
            ]);
            
            if (!empty($items)) {
                return $items[0]->getId();
            }
            
            return null;
            
        } catch (Exception $e) {
            error_log("Ошибка при поиске бренда: " . $e->getMessage());
            return null;
        }
    }
}

class ImageProcessor {
    private $maxWidth;
    private $maxHeight;
    
    public function __construct($maxWidth = 1000, $maxHeight = 1000) {
        $this->maxWidth = $maxWidth;
        $this->maxHeight = $maxHeight;
    }
    
    public function getResizedImageUrl($originalImageUrl, $customMaxWidth = null, $customMaxHeight = null) {
        $maxWidth = $customMaxWidth ?: $this->maxWidth;
        $maxHeight = $customMaxHeight ?: $this->maxHeight;
        
        // Получаем информацию о размере изображения
        $imageInfo = $this->getImageInfo($originalImageUrl);
        
        if (!$imageInfo) {
            return $originalImageUrl; // Возвращаем оригинальный URL если не удалось получить информацию
        }
        
        $originalWidth = $imageInfo['width'];
        $originalHeight = $imageInfo['height'];
        
        // Проверяем, нужно ли уменьшение
        if ($originalWidth <= $maxWidth && $originalHeight <= $maxHeight) {
            return $originalImageUrl;
        }
        
        // Вычисляем соотношение сторон для уменьшенной версии
        $aspectRatio = $originalWidth / $originalHeight;
        
        // Вычисляем новые размеры с сохранением соотношения сторон
        if ($originalWidth > $originalHeight) {
            $newWidth = $maxWidth;
            $newHeight = round($maxWidth / $aspectRatio);
        } else {
            $newHeight = $maxHeight;
            $newWidth = round($maxHeight * $aspectRatio);
        }
        
        // Гарантируем, что размеры не превышают максимальные
        if ($newWidth > $maxWidth) {
            $newWidth = $maxWidth;
            $newHeight = round($maxWidth / $aspectRatio);
        }
        
        if ($newHeight > $maxHeight) {
            $newHeight = $maxHeight;
            $newWidth = round($maxHeight * $aspectRatio);
        }
        
        // Формируем URL для уменьшенного изображения
        return "RESIZED_PHOTOS/{$newWidth}X{$newHeight}";
    }
    
    public function getImageInfo($imageUrl) {
        $imageInfo = @getimagesize($imageUrl);
        
        if (!$imageInfo) {
            return null;
        }
        
        return [
            'width' => $imageInfo[0],
            'height' => $imageInfo[1],
            'type' => $imageInfo[2],
            'mime' => $imageInfo['mime'],
            'aspect_ratio' => $imageInfo[0] / $imageInfo[1]
        ];
    }
    
    public function downloadImage($url) {
        $context = stream_context_create([
            'http' => [
                'method' => 'GET',
                'header' => "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n",
                'timeout' => 30
            ],
            'ssl' => [
                'verify_peer' => false,
                'verify_peer_name' => false,
            ]
        ]);
        
        $imageData = @file_get_contents($url, false, $context);
        
        if ($imageData === false) {
            throw new Exception("Не удалось загрузить изображение: {$url}");
        }
        
        return [
            'data' => $imageData,
            'url' => $url
        ];
    }
    
    public function processImageForBitrix($imageUrl, $maxWidth = 1000, $maxHeight = 1000) {
        try {
            $resizedUrl = $this->getResizedImageUrl($imageUrl, $maxWidth, $maxHeight);
            $imageData = $this->downloadImage($resizedUrl);
            
            return [
                'success' => true,
                'data' => $imageData,
                'url' => $resizedUrl,
                'base64' => base64_encode($imageData['data'])
            ];
            
        } catch (Exception $e) {
            return [
                'success' => false,
                'error' => $e->getMessage(),
                'url' => $imageUrl
            ];
        }
    }
}

class ApiClient {
    private $username;
    private $password;
    private $baseUrl;
    
    public function __construct($username, $password, $baseUrl) {
        $this->username = $username;
        $this->password = $password;
        $this->baseUrl = $baseUrl;
    }
    
    public function makeRequest($endpoint, $method = 'GET', $data = null) {
        $url = $this->baseUrl . $endpoint;
        
        // Подготовка опций для stream context
        $headers = [
            'Content-Type: application/json',
            'Authorization: Basic ' . base64_encode($this->username . ':' . $this->password),
            'User-Agent: PHP-Script/1.0'
        ];
        
        $options = [
            'http' => [
                'method' => $method,
                'header' => implode("\r\n", $headers),
                'timeout' => 30,
                'ignore_errors' => true // Чтобы получать HTTP коды ошибок
            ]
        ];
        
        // Добавляем данные для POST/PUT запросов
        if (in_array($method, ['POST', 'PUT']) && $data !== null) {
            $options['http']['content'] = json_encode($data);
        }
        
        $context = stream_context_create($options);
        
        try {
            $response = file_get_contents($url, false, $context);
            $httpCode = $this->getHttpCode($http_response_header);
            
            return [
                'success' => ($httpCode >= 200 && $httpCode < 300),
                'http_code' => $httpCode,
                'response' => $response,
                'error' => null
            ];
            
        } catch (Exception $e) {
            return [
                'success' => false,
                'http_code' => 0,
                'response' => null,
                'error' => $e->getMessage()
            ];
        }
    }
    
    private function getHttpCode($headers) {
        if (is_array($headers) && count($headers) > 0) {
            // Первая строка содержит статус HTTP
            $statusLine = $headers[0];
            preg_match('{HTTP/\d\.\d\s+(\d+)\s+.*}', $statusLine, $match);
            return isset($match[1]) ? (int)$match[1] : 0;
        }
        return 0;
    }
}

function fetchAllData() {
	$api_username = '';
	$api_password = '';
	$api_base_url = '';

    $client = new ApiClient($api_username, $api_password, $api_base_url);
    $results = [];
/*
    $warehousesResult = $client->makeRequest('warehouses', 'GET');
    $results['warehouses'] = $warehousesResult['success'] ? json_decode($warehousesResult['response'], JSON_UNESCAPED_UNICODE) : null;

    $itemsResult = $client->makeRequest('items', 'GET');
    $results['items'] = $itemsResult['success'] ? json_decode($itemsResult['response'], JSON_UNESCAPED_UNICODE) : null;

    $clientsResult = $client->makeRequest('clients', 'GET');
    $results['clients'] = $clientsResult['success'] ? json_decode($clientsResult['response'], JSON_UNESCAPED_UNICODE) : null;


    $itemsResult = $client->makeRequest('cards', 'GET');
    $results['cards'] = $itemsResult['success'] ? json_decode($itemsResult['response'], JSON_UNESCAPED_UNICODE) : null;
*/
    $purchasesResult = $client->makeRequest('purchases', 'GET');
    $results['purchases'] = $purchasesResult['success'] ? json_decode($purchasesResult['response'], JSON_UNESCAPED_UNICODE) : null;


    return $results;
}

class DateManager {
    public function formatDate($isoDate){
        if (empty($isoDate)) {
            return null;
        }
        
        $dateObject = \DateTime::createFromFormat('Y-m-d\\TH:i:s', $isoDate);
        if ($dateObject === false) {
            return null;
        } else {
            $formattedDate = $dateObject->format('d.m.Y H:i:s');
            return $formattedDate;
        }
    }
}

class BrandManager {
    private $entityManager;
    
    public function __construct(EntityManager $entityManager) {
        $this->entityManager = $entityManager;
    }
    
    /**
     * Создает бренд в смарт-процессе если его не существует
     * @param string $brandName Название бренда
     * @param string $brandCode Код бренда
     * @return int|false ID созданного бренда или false при ошибке
     */
    public function createBrandIfNotExists($brandName, $brandCode) {
        if (empty($brandName)) {
            error_log("Название бренда не может быть пустым");
            return false;
        }
        
        // Проверяем существование бренда
        $existingBrand = $this->findBrandByCode($brandCode);
        if ($existingBrand) {
            return $existingBrand['ID'];
        }
        
        // Создаем новый бренд
        $brandFields = [
            'TITLE' => $brandName,
            'UF_CRM_6_1759316082823' => $brandName,
            'UF_CRM_6_CODE' => $brandCode,
        ];
        
        $brandId = $this->entityManager->createSp($brandFields, 1052);
        
        if ($brandId) {
            error_log("Создан новый бренд: {$brandName} (ID: {$brandId})");
        } else {
            error_log("Ошибка при создании бренда: {$brandName}");
        }
        
        return $brandId;
    }
    
    /**
     * Ищет бренд по названию
     * @param string $brandName Название бренда
     * @return array|false Данные бренда или false если не найден
     */
    private function findBrandByCode($brandCode) {
        try {
            $factory = Service\Container::getInstance()->getFactory(1052);
            if (!$factory) {
                error_log("Фабрика для смарт-процесса брендов (1052) не найдена");
                return false;
            }
            
            $items = $factory->getItems([
                'filter' => [
                    '=UF_CRM_6_CODE' => $brandCode
                ],
                'limit' => 1
            ]);
            
            if (!empty($items)) {
                $brand = $items[0];
                return [
                    'ID' => $brand->getId(),
                    'TITLE' => $brand->getTitle()
                ];
            }
            
            return false;
            
        } catch (Exception $e) {
            error_log("Ошибка при поиске бренда: " . $e->getMessage());
            return false;
        }
    }
}

class EntityManager {
    private $dateManager;
    private $imageProcessor;
    private $brandManager;
    private $logger;

    public function __construct(DateManager $dateManager = null, ImageProcessor $imageProcessor = null, JsonLogger $logger = null) {
        $this->dateManager = $dateManager ?: new DateManager();
        $this->imageProcessor = $imageProcessor ?: new ImageProcessor();
        $this->brandManager = new BrandManager($this);
        $this->logger = $logger ?: new JsonLogger();
    }

    public function processItemImage($imageUrl, $entityId = null) {
        if (empty($imageUrl)) {
            return null;
        }
        
        try {
            $imageResult = $this->imageProcessor->processImageForBitrix($imageUrl, 1000, 1000);
            
            if ($imageResult['success']) {
                return [
                    'fileData' => [
                        'item_image.jpg',
                        $imageResult['base64']
                    ]
                ];
            } else {
                $this->logger->logPhotoError('item', $entityId, $imageUrl, $imageResult['error']);
                return null;
            }
        } catch (Exception $e) {
            $this->logger->logPhotoError('item', $entityId, $imageUrl, $e->getMessage());
            return null;
        }
    }

    public function createDeal($entityFields){
        $entityObject = new \CCrmDeal(false);

        $entityId = $entityObject->Add(
            $entityFields,
            true, // $bUpdateSearch
            []    // $arOptions
        );

        if (!$entityId) {
            if (method_exists($entityObject, 'GetLAST_ERROR')) {
                error_log("Ошибка при создании сделки: " . $entityObject->GetLAST_ERROR());
            } else {
                error_log("Ошибка при создании сделки");
            }
            return false;
        }

        $relationManager = new DealRelationManager($this, $this->logger);
        $relations = $relationManager->findAndAttachRelationsToDeal($entityId, $entityFields);

        if (!empty($entityFields['UF_CRM_1759317764974'])) { // item_name
            $this->findAndAddProductToDeal(
                $entityId, 
                $entityFields['UF_CRM_1759317764974'], // item_name
                $entityFields['UF_CRM_1759317788432'] ?? 1, // count
                $entityFields['OPPORTUNITY'],
            );
        }
        $entity = new Bitrix\Crm\DealTable();
        $res = $entity->update($entityId, ['OPPORTUNITY' => $entityFields['OPPORTUNITY']]);
        return $entityId;
    }
/**
 * Находит товар по свойству 65 (название) и добавляет его в сделку
 */
public function findAndAddProductToDeal($dealId, $itemName, $count, $price) {
    try {
        // Ищем товар по свойству 65 (название)
        $product = $this->findProductByProperty65($itemName);
        
        if (!$product) {
            $this->logger->logMappingError('deal_product', $dealId, 'item_name', $itemName, [
                'deal_id' => $dealId,
                'item_name' => $itemName,
                'message' => 'Товар не найден по названию'
            ]);
            return false;
        }
        
        // Добавляем товар в сделку
        $result = $this->addProductToDeal($dealId, $product, $count, $price);
        
        if ($result) {
            $this->updateDealOpportunity($dealId, $dealSum);
            /*
            $this->logger->logSuccess('deal_product', $dealId, "Товар добавлен в сделку: {$product['ID']}", [
                'deal_id' => $dealId,
                'product_id' => $product['ID'],
                'product_name' => $itemName,
                'count' => $count,
                'price' => $product['PRICE'] ?? 0
            ]);*/
        }
        
        return $result;
        
    } catch (Exception $e) {
        $this->logger->logGeneralError('deal_product', $dealId, "Ошибка добавления товара в сделку: " . $e->getMessage(), [
            'deal_id' => $dealId,
            'item_name' => $itemName,
            'count' => $count
        ]);
        return false;
    }
}
private function updateDealOpportunity($dealId, $sum) {
        $deal = new \CCrmDeal(false);
        $updateFields = [
            'OPPORTUNITY' => (float)$sum
        ];
        
        $result = $deal->Update($dealId, $updateFields);
}

/**
 * Ищет товар по свойству 65 (название)
 */
private function findProductByProperty65($itemName) {
    try {
        $result = CRest::call('catalog.product.list', [
            'filter' => [
                '=property65' => $itemName,
                'iblockId' => 14
            ],
            'select' => [
                'id',
                'name',
                'price',
                'property65',
                'iblockId'
            ]
        ]);

        if (isset($result['result']['products']) && !empty($result['result']['products'])) {
            return $result['result']['products'][0];
        }
        
        return null;
        
    } catch (Exception $e) {
        error_log("Ошибка при поиске товара: " . $e->getMessage());
        return null;
    }
}

/**
 * Добавляет товар в сделку через productrows.set
 */
private function addProductToDeal($dealId, $product, $count, $price) {
    try {
        // Получаем цену товара
        //$price = $product['price'] ?? 0;

        // Формируем данные для добавления товара
        $productRows = [
            [
                'PRODUCT_ID' => $product['id'],
                'PRODUCT_NAME' => $product['name'],
                'QUANTITY' => (float)$count,
                'PRICE' => $price,
                'TAX_INCLUDED' => 'Y'
            ]
        ];

        $result = CRest::call('crm.deal.productrows.set', [
            'id' => $dealId,
            'rows' => $productRows
        ]);

        return isset($result['result']) && $result['result'] === true;
        
    } catch (Exception $e) {
        error_log("Ошибка при добавлении товара в сделку: " . $e->getMessage());
        return false;
    }
}
    public function makeBitrixRequest($fields){
        $result = CRest::call(
            'catalog.product.add',
            [
                'fields' => $fields
            ]
        );

        return $result;
    }

    public function createSp($entityFields, $entityTypeId){
        try {
            $factory = Service\Container::getInstance()->getFactory($entityTypeId);
            if (!$factory) {
                error_log("Фабрика для типа сущности $entityTypeId не найдена");
                return false;
            }
            
            $item = $factory->createItem();
            $item->setFromCompatibleData($entityFields);
            
            $operation = $factory->getAddOperation($item);
            $operationResult = $operation->launch();

            if ($operationResult->isSuccess()){
                return $item->getId();
            } else {
                $errors = $operationResult->getErrorMessages();
                error_log("Ошибка при создании сущности: " . implode(", ", $errors));
                return false;
            }
        } catch (Exception $e) {
            error_log("Исключение при создании сущности: " . $e->getMessage());
            return false;
        }
    }

    public function addEntity($entity, $item){
        if (empty($item)) {
            $this->logger->logGeneralError($entity, 'unknown', "Пустые данные для сущности");
            return false;
        }

        $entityId = $item["code"] ?? $item["number"] ?? $item["receipt_number"] ?? 'unknown';

        // Проверка обязательных полей и логирование всех пустых полей
        $this->checkRequiredFields($entity, $item, $entityId);
        
        // Дополнительная проверка всех полей на пустоту
        $this->checkAllFieldsForEmpty($entity, $item, $entityId);

        switch ($entity) {
            case 'deal':
                // Проверяем наличие пользователя (cashier_code)
                if (!empty($item["cashier_code"])) {
                    $userExists = $this->checkUserExists($item["cashier_code"]);
                    if (!$userExists) {
                        $this->logger->logUserNotFound('deal', $entityId, $item["cashier_code"], $item);
                    }
                }

                $entityFields = [
                    'TITLE' => $item["product_name"] ?? 'Без названия',
                    'OPPORTUNITY' => $item["sum"] ?? 0,
                    'UF_CRM_1756711109104' => $item["receipt_number"] ?? '',
                    'UF_CRM_1756711204935' => $item["register"] ?? '',
                    'UF_CRM_1760529583' => $this->dateManager->formatDate($item["date"] ?? ''),
                    'UF_CRM_1756713651' => $item["warehouse_code"] ?? '',
                    'UF_CRM_1761200403' => $item["warehouse_code"] ?? '',
                    //'UF_CRM_1759317671' => $item["cashier_code"] ?? '',
                    'UF_CRM_1761200470'  => $item["cashier_code"] ?? '',
                    'UF_CRM_1756712343' => $item["card_number"] ?? '',
                    'UF_CRM_1761200496' => $item["card_number"] ?? '',
                    'UF_CRM_1759321212833' => $item["product_name"] ?? '',
                    'UF_CRM_1759317764974' => $item["item_name"] ?? '',
                    'UF_CRM_1759317788432' => $item["count"] ?? 0,
                    'UF_CRM_1759317801939' => $item["weight"] ?? 0,
                    'STAGE_ID' => 'NEW',
                    'CURRENCY_ID' => 'RUB',
                    'IS_MANUAL_OPPORTUNITY' => 'Y',
                ];

                $result = $this->createDeal($entityFields);

                if ($result) {
                   // $this->logger->logSuccess('deal', $entityId, $result, $item);
                } else {
                    $this->logger->logGeneralError('deal', $entityId, "Ошибка при создании сделки", $item);
                }
                
                return $result;
                
            case 'card':
                $factory = Service\Container::getInstance()->getFactory(\CCrmOwnerType::Contact);
                if (!$factory) {
                    $this->logger->logGeneralError('card', $entityId, "Фабрика контактов не найдена", $item);
                    return false;
                }
                
                $clientId = null;
                if (!empty($item["client"])) {
                    $items = $factory->getItems([
                        'filter' => [
                            '=UF_CRM_1760599281' => $item["client"]
                        ],
                        'limit' => 1
                    ]);
                    
                    if (!empty($items)) {
                        $clientId = $items[0]->getId();
                    } else {
                        $this->logger->logMappingError('card', $entityId, 'client', $item["client"], $item);
                    }
                }

                $entityFields = [
                    'TITLE' => $item["number"] ?? 'Без названия',
                    'UF_CRM_3_1759320971349' => $item["number"] ?? '',
                    'UF_CRM_3_CLIENT' => $clientId ?? '',
                    'UF_CRM_3_1759315419431' => $item["is_blocked"] ?? 0,
                    'UF_CRM_3_1760598978' => $item["client"] ?? 0,
                    'UF_CRM_3_1759317288635' => $this->dateManager->formatDate($item["application_date"] ?? ''),
                    'UF_CRM_3_1760598832' => $item["warehouse_code"] ?? '',
                    'UF_CRM_3_1760598956' => $item["discount_card_type"] ?? ''
                ];
                
                $result = $this->createSp($entityFields, 1038);
                
                if ($result) {
                   // $this->logger->logSuccess('card', $entityId, $result, $item);
                } else {
                    $this->logger->logGeneralError('card', $entityId, "Ошибка при создании карты", $item);
                }
                
                return $result;
                
            case 'item':
                // Сначала создаем/проверяем бренд
                $brandId = null;
                if (!empty($item["brand_code"])) {
                    $brandName = $item["brand_name"] ?? $item["brand_code"];
                    $brandId = $this->brandManager->createBrandIfNotExists($brandName, $item["brand_code"]);
                    if (!$brandId) {
                        $this->logger->logMappingError('item', $entityId, 'brand_code', $item["brand_code"], $item);
                    }
                }
                
                // Обрабатываем основное изображение товара
                $detailPicture = null;
                if (!empty($item["product_image_filename"])) {
                    $detailPicture = $this->processItemImage(
                        "" . $item["product_image_filename"],
                        $entityId
                    );
                }
                
                // Обрабатываем дополнительное фото
                $nimPhoto1 = null;
                if (!empty($item["nim_photo1"])) {
                    $nimPhoto1 = $this->processItemImage(
                        "" . $item["nim_photo1"],
                        $entityId
                    );
                }

                $fields = [
                    'code' => $item["code"] ?? '',
                    'property65' => $item["name"] ?? '',
                    'property66' => $item["uin"] ?? '',
                    'name' => $item["product_name"] ?? '',
                    'property67' => $item["product_sku"] ?? '',
                    'detailPicture' => $detailPicture ?? $nimPhoto1 ?? '',
                    'property68' => $item["nim_photo1"] ?? '',
                    'property79' => $item["product_image_filename"] ?? '',
                    'property78' => $item["brand_code"] ?? '',
                    'property70' => $item["size_name"] ?? '',
                    'property72' => $item["metal_name"] ?? '',
                    'property74' => $item["fineness_name"] ?? '',
                    'property73' => $item["feature_name"] ?? '',
                    'iblockId' => 14,
                    'iblockSectionId' => 13
                ];

                // Добавляем ID бренда если найден
                if ($brandId) {
                    $fields['property77'] = $brandId;
                }

                $result = $this->makeBitrixRequest($fields);
                
                // Для товаров логируем отдельно, так как они создаются через batch
                if ($result && isset($result['result']) && $result['result'] > 0) {
                   // $this->logger->logSuccess('item', $entityId, $result['result'], $item);
                } else {
                    $error = isset($result['error']) ? $result['error'] : 'Неизвестная ошибка';
                    $this->logger->logGeneralError('item', $entityId, $error, $item);
                }
                
                return $result;
                
            case 'brand':
                $entityFields = [
                    'TITLE' => $item["name"] ?? 'Без названия',
                    'UF_CRM_6_1759316082823' => $item["name"] ?? '',
                    'UF_CRM_6_CODE' => $item["code"] ?? ''
                ];
                
                $result = $this->createSp($entityFields, 1052);
                
                if ($result) {
                   // $this->logger->logSuccess('brand', $entityId, $result, $item);
                } else {
                    $this->logger->logGeneralError('brand', $entityId, "Ошибка при создании бренда", $item);
                }
                
                return $result;
                
            default:
                $this->logger->logGeneralError($entity, $entityId, "Неизвестный тип сущности", $item);
                return false;
        }
    }

    /**
     * Проверяет обязательные поля и логирует пустые
     */
    public function checkRequiredFields($entity, $item, $entityId) {
        $requiredFields = $this->getRequiredFields($entity);
        $emptyFields = [];
        
        foreach ($requiredFields as $field) {
            if (empty($item[$field])) {
                $emptyFields[$field] = $item[$field] ?? null;
            }
        }
        
        if (!empty($emptyFields)) {
            $this->logger->logMultipleEmptyFields($entity, $entityId, $emptyFields, $item);
        }
    }
    
    /**
     * Проверяет все поля на пустоту и логирует их
     */
    public function checkAllFieldsForEmpty($entity, $item, $entityId) {
        $emptyFields = [];
        
        foreach ($item as $fieldName => $fieldValue) {
            if (empty($fieldValue) && $fieldValue !== 0 && $fieldValue !== '0') {
                $emptyFields[$fieldName] = $fieldValue;
            }
        }
        
        if (!empty($emptyFields)) {
            $this->logger->logMultipleEmptyFields($entity, $entityId, $emptyFields, $item);
        }
    }
    
    /**
     * Возвращает список обязательных полей для каждого типа сущности
     */
    private function getRequiredFields($entity) {
        $requiredFields = [
            'deal' => ['receipt_number', 'sum', 'date'],
            'card' => ['number', 'client'],
            'item' => ['code', 'product_name'],
            'brand' => ['name', 'code']
        ];
        
        return $requiredFields[$entity] ?? [];
    }
    
    /**
     * Проверяет существование пользователя на портале
     */
    private function checkUserExists($cashierCode) {
        try {
            $result = CRest::call('user.search', [
                'FILTER' => [
                    'UF_DEPARTMENT' => false,
                    'UF_CRM_1756713651' => $cashierCode
                ]
            ]);
            
            return isset($result['result']) && !empty($result['result']);
            
        } catch (Exception $e) {
            error_log("Ошибка при проверке пользователя: " . $e->getMessage());
            return false;
        }
    }
}

function main() {
    $logger = new JsonLogger();
    $logger->clearLog(); // Очищаем лог перед началом импорта
    
    echo "Начинаем импорт данных...\n";
    
    $data = fetchAllData();
    $entityManager = new EntityManager(new DateManager(), new ImageProcessor(), $logger);

    if (!empty($data['cards'])) {
        echo "Синхронизируем карты...\n";
        $cardSyncManager = new CardSyncManager($entityManager, $logger);
        $syncResults = $cardSyncManager->syncCards($data['cards']);
        
        echo "Синхронизация карт завершена:\n";
        echo "- Создано: " . count($syncResults['created']) . "\n";
        echo "- Обновлено: " . count($syncResults['updated']) . "\n";
        echo "- Ошибок: " . count($syncResults['errors']) . "\n";
    }

    $productsToCreate = [];
    foreach ($data['items'] ?? [] as $index => $item) {
        if ($index >= 2400 && $index < 3333) {
            $productsToCreate[] = $item;
        }
    }

    echo "Найдено товаров для обработки: " . count($productsToCreate) . "\n";

    // Создаем товары через batch запросы
    //$results = $batchManager->createProductsBatch($productsToCreate);

    $results = [];
    echo "<pre>";
    print_r($data);
    echo "</pre>";
    // Обработка товаров через batch менеджер
    if (!empty($data['items'])) {
        echo "Обрабатываю товары...\n";
        $batchManager = new BatchProductManager($entityManager, 50, $logger);
        $results['items'] = $batchManager->createProductsBatch($productsToCreate);
    }

    // Обработка других сущностей
    $entities = [
        'warehouses' => 'deal',
        'clients' => 'card',
        'purchases' => 'deal'
    ];
    
    foreach ($entities as $dataKey => $entityType) {
        if (!empty($data[$dataKey])) {
            echo "Обрабатываю {$entityType}...\n";
            foreach ($data[$dataKey] as $index => $item) {
                if($index > 2400){
                $results[$dataKey][] = $entityManager->addEntity($entityType, $item);
                }
            }
        }
    }
    
    // Выводим статистику
    $stats = $logger->getStats();
    $logInfo = $logger->getLogInfo();
    
    echo "\n=== СТАТИСТИКА ИМПОРТА ===\n";
    echo "Всего записей в логе: {$stats['total']}\n";
    echo "Успешных операций: {$stats['success']}\n";
    echo "Пустых полей: {$stats['empty_field']}\n";
    echo "Ошибок сопоставления: {$stats['mapping_error']}\n";
    echo "Ошибок фото: {$stats['photo_error']}\n";
    echo "Пользователей не найдено: {$stats['user_not_found']}\n";
    echo "Общих ошибок: {$stats['general_error']}\n";
    
    echo "\n=== ИНФОРМАЦИЯ О ЛОГ ФАЙЛЕ ===\n";
    echo "Статус: {$logInfo['message']}\n";
    echo "Размер файла: {$logInfo['file_size']} байт\n";
    echo "Количество записей: {$logInfo['entries_count']}\n";
    echo "Путь к файлу: {$logInfo['file_path']}\n";
    
    if ($logInfo['success']) {
        echo "\n✅ Логирование успешно настроено и работает!\n";
    } else {
        echo "\n❌ Внимание: есть проблемы с логированием!\n";
    }
    
    return $results;
}
function processRecentPurchases() {
    $logger = new JsonLogger();
    $entityManager = new EntityManager(new DateManager(), new ImageProcessor(), $logger);
    
    echo "Обработка недавних покупок...\n";
    
    // Получаем только недавние покупки
    $recentPurchases = fetchRecentPurchasesOnly();
    
    if (empty($recentPurchases)) {
        echo "Нет покупок за последние 3 минуты\n";
        return;
    }
    
    echo "Найдено покупок за последние 3 минуты: " . count($recentPurchases) . "\n";
    
    $results = [
        'created' => [],
        'errors' => []
    ];
    
    // Создаем сделки для каждой недавней покупки
    foreach ($recentPurchases as $purchase) {
        $entityId = $purchase["receipt_number"] ?? 'unknown';
        
        try {
            echo "Создаю сделку для покупки: {$entityId}\n";
            
            $result = $entityManager->addEntity('deal', $purchase);
            
            if ($result) {
                $results['created'][] = [
                    'receipt_number' => $entityId,
                    'deal_id' => $result
                ];
                echo "✅ Сделка создана: {$result} для чека {$entityId}\n";
            } else {
                $results['errors'][] = [
                    'receipt_number' => $entityId,
                    'error' => 'Ошибка создания сделки'
                ];
                echo "❌ Ошибка создания сделки для чека {$entityId}\n";
            }
            
        } catch (Exception $e) {
            $results['errors'][] = [
                'receipt_number' => $entityId,
                'error' => $e->getMessage()
            ];
            echo "❌ Исключение при создании сделки для чека {$entityId}: " . $e->getMessage() . "\n";
        }
    }
    
    // Логируем результаты
    $logger->logGeneralError('update_deal', 'batch', "Обработано недавних покупок", [
        'total_recent' => count($recentPurchases),
        'created' => count($results['created']),
        'errors' => count($results['errors']),
        'results' => $results
    ]);
    
    echo "\n=== РЕЗУЛЬТАТЫ ОБРАБОТКИ ===\n";
    echo "Всего недавних покупок: " . count($recentPurchases) . "\n";
    echo "Успешно создано сделок: " . count($results['created']) . "\n";
    echo "Ошибок: " . count($results['errors']) . "\n";
    
    return $results;
}

function filterRecentPurchases($purchases) {
    $recentPurchases = [];
    $threeMinutesAgo = new DateTime('-3 minutes');
    
    foreach ($purchases as $purchase) {
        if (empty($purchase['date'])) {
            continue;
        }
        
        // Парсим дату из формата "2025-05-19T20:03:56"
        $purchaseDate = DateTime::createFromFormat('Y-m-d\TH:i:s', $purchase['date']);
        
        if ($purchaseDate === false) {
            continue;
        }

        // Проверяем, находится ли дата в пределах последних 3 минут
        if ($purchaseDate >= $threeMinutesAgo) {
            $recentPurchases[] = $purchase;
        }
    }
    
    return $recentPurchases;
}

// Альтернативная версия с оптимизированным запросом только для покупок
function fetchRecentPurchasesOnly() {
	$api_username = '';
	$api_password = '';
	$api_base_url = '';

    $client = new ApiClient($api_username, $api_password, $api_base_url);
    
    // Вычисляем дату 3 минуты назад в нужном формате
    $threeMinutesAgo = new DateTime('-3 minutes');
    $filterDate = $threeMinutesAgo->format('Y-m-d\TH:i:s');

    
    $purchasesResult = $client->makeRequest('purchases', 'GET');
    
    if ($purchasesResult['success']) {
        $allPurchases = json_decode($purchasesResult['response'], JSON_UNESCAPED_UNICODE);
        // Фильтруем на стороне PHP если API не поддерживает фильтрацию
        return filterRecentPurchases($allPurchases);
    }
    
    return [];
}
    main();
/*
if(strpos($_SERVER['REQUEST_URI'], 'update_deal') !== false){
    processRecentPurchases();
} elseif(strpos($_SERVER['REQUEST_URI'], 'update') !== false){
    compareAndUpdateData();
} else {
    main();
}
*/
//$client = new ApiClient($api_username, $api_password, $api_base_url);
//$itemsResult = $client->makeRequest('clients/changes&message_number=256832', 'DELETE',);

?>