<?php
\Bitrix\Main\Loader::includeModule("crm");
use \Bitrix\Main,
	\Bitrix\Crm\Service,
    \Bitrix\Crm\DealTable;
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
        $brandName = '';
        
        if (!empty($item["brand_code"])) {
            // Пытаемся найти название бренда в Bitrix через EntityManager
            $brandName = $this->entityManager->findBrandNameByCode($item["brand_code"]);
            
            // Если не нашли в Bitrix, используем данные из кэша
            if (empty($brandName)) {
                $brandName = $item["brand_name"] ?? $item["brand_code"];
            }
            
            $brandId = $this->findBrandIdByCode($item["brand_code"]);
        }
        
        // Обрабатываем изображения
        $detailPicture = null;
        if (!empty($item["product_image_filename"])) {
            $detailPicture = $this->entityManager->processItemImage(
                "https://media.trimiata.ru/SOURCES/PHOTO/" . $item["product_image_filename"],
                $item["code"] ?? 'unknown'
            );
        }
        
        $nimPhoto1 = null;
        if (!empty($item["nim_photo1"])) {
            $nimPhoto1 = $this->entityManager->processItemImage(
                "https://media.trimiata.ru/SOURCES/PHOTO/" . $item["nim_photo1"],
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
                    'UF_CRM_6_CODE' => $brandCode
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
        $this->mediaConfig = $this->getMediaConfig();
    }

    private function getMediaConfig() {
        $config = getApiConfig();
        return $config['media'];
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
        $resizedBase = $this->mediaConfig['base_url'] . $this->mediaConfig['resized_photos_path'];
        return $resizedBase . "{$newWidth}X{$newHeight}";
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

function getApiConfig() {
    static $config = null;
    
    if ($config === null) {
        $configFile = __DIR__ . '/config.php';
        if (file_exists($configFile)) {
            $config = require $configFile;
        } else {
            // Fallback к значениям по умолчанию
            $config = [
                'api' => [
                    'username' => '',
                    'password' => '',
                    'base_url' => ''
                ],
                'media' => [
                    'base_url' => '',
                    'photos_path' => '',
                    'resized_photos_path' => ''
                ]
            ];
        }
    }
    
    return $config;
}

function getApiCredentials() {
    $config = getApiConfig();
    return $config['api'] ?? [];
}

function getMediaConfig() {
    $config = getApiConfig();
    return $config['media'] ?? [];
}

function createApiClient() {
    $apiConfig = getApiCredentials();
    return new ApiClient(
        $apiConfig['username'] ?? '', 
        $apiConfig['password'] ?? '', 
        $apiConfig['base_url'] ?? ''
    );
}

function fetchAllData() {
    $apiConfig = getApiCredentials();
	$api_username = $apiConfig['username'];
	$api_password = $apiConfig['password'];
	$api_base_url = $apiConfig['base_url'];

    $client = new ApiClient($api_username, $api_password, $api_base_url);
    $results = [];

    $warehousesResult = $client->makeRequest('warehouses', 'GET');
    $results['warehouses'] = $warehousesResult['success'] ? json_decode($warehousesResult['response'], JSON_UNESCAPED_UNICODE) : null;

    $clientsResult = $client->makeRequest('clients', 'GET');
    $results['clients'] = $clientsResult['success'] ? json_decode($clientsResult['response'], JSON_UNESCAPED_UNICODE) : null;


    $itemsResult = $client->makeRequest('cards', 'GET');
    $results['cards'] = $itemsResult['success'] ? json_decode($itemsResult['response'], JSON_UNESCAPED_UNICODE) : null;

    $itemsResult = $client->makeRequest('items', 'GET');
    $results['items'] = $itemsResult['success'] ? json_decode($itemsResult['response'], JSON_UNESCAPED_UNICODE) : null;

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
    private $cachedData;

    public function __construct(DateManager $dateManager = null, ImageProcessor $imageProcessor = null, JsonLogger $logger = null) {
        $this->dateManager = $dateManager ?: new DateManager();
        $this->imageProcessor = $imageProcessor ?: new ImageProcessor();
        $this->brandManager = new BrandManager($this);
        $this->logger = $logger ?: new JsonLogger();
        $this->cachedData = null;
    }

    private function getCachedData() {
        if ($this->cachedData === null) {
            $this->cachedData = fetchAllData();
        }
        return $this->cachedData;
    }

    private function findCardInCachedData($cardNumber) {
        $data = $this->getCachedData();
        $cards = $data['cards'] ?? [];
        
        foreach ($cards as $card) {
            if (($card['number'] ?? '') === $cardNumber) {
                return $card;
            }
        }
        return null;
    }
    public function createOrFindProductByNameWithWeight($itemName, $weight) {
        try {
            // Сначала ищем существующий товар
            $product = $this->findProductByProperty65($itemName);
            if ($product) {
                return $product['id'];
            }
            
            // Ищем товар в кешированных данных для получения дополнительной информации
            $cachedItem = $this->findItemInCachedData($itemName);
            
            if ($cachedItem) {
                // Используем данные из кэша для создания товара с учетом веса
                $productFields = $this->prepareProductFieldsFromCachedData($cachedItem, $weight);
                $result = $this->makeBitrixRequest($productFields);
                
                if ($result && isset($result['result']) && $result['result'] > 0) {
                    return $result['result'];
                }
            } else {
                // Создаем новый товар с минимальными данными, включая вес
                $productFields = [
                    'name' => $itemName,
                    'property65' => $itemName,
                    'property85' => $weight, // Добавляем вес
                    'iblockId' => 14,
                    'iblockSectionId' => 13,
                    'active' => 'Y'
                ];

                $result = $this->makeBitrixRequest($productFields);
                
                if ($result && isset($result['result']) && $result['result'] > 0) {
                    return $result['result'];
                }
            }

            return null;

        } catch (Exception $e) {
            error_log("Ошибка при создании товара по названию с весом: " . $e->getMessage());
            return null;
        }
    }
    /**
     * Находит клиента в кешированных данных
     */
    private function findClientInCachedData($clientCode) {
        $data = $this->getCachedData();
        $clients = $data['clients'] ?? [];
        
        foreach ($clients as $client) {
            if (($client['code'] ?? '') === $clientCode) {
                return $client;
            }
        }
        return null;
    }

    /**
     * Находит склад в кешированных данных по коду
     */
    private function findWarehouseInCachedData($warehouseCode) {
        $data = $this->getCachedData();
        $warehouses = $data['warehouses'] ?? [];
        
        foreach ($warehouses as $warehouse) {
            if (($warehouse['code'] ?? '') === $warehouseCode) {
                return $warehouse;
            }
        }
        return null;
    }

    /**
     * Находит товар в кешированных данных
     */
    private function findItemInCachedData($itemName) {
        $data = $this->getCachedData();
        $items = $data['items'] ?? [];

        foreach ($items as $item) {
            if ($item['name'] === $itemName) {
                return $item;
            }
        }
        return null;
    }

    /**
     * Переопределяем метод createOrFindRelatedEntities для использования кешированных данных
     */
    private function createOrFindRelatedEntities($dealFields) {
        $relatedEntities = [];

        // 1. Создаем/находим карту с использованием кешированных данных
        if (!empty($dealFields['UF_CRM_1756712343'])) { // card_number
            $cardId = $this->createOrFindCardWithCachedData($dealFields);
            if ($cardId) {
                $relatedEntities['card_id'] = $cardId;
            }
        }

        // 2. Создаем/находим склад/магазин с использованием кешированных данных
        if (!empty($dealFields['UF_CRM_1756713651'])) { // warehouse_code
            $warehouseId = $this->createOrFindWarehouseWithCachedData($dealFields);
            if ($warehouseId) {
                $relatedEntities['warehouse_id'] = $warehouseId;
            }
        }

        // 3. Создаем/находим товар с использованием кешированных данных
        if (!empty($dealFields['UF_CRM_1759317764974'])) { // item_name
            $productId = $this->createOrFindProductWithCachedData($dealFields);
            if ($productId) {
                $relatedEntities['product_id'] = $productId;
            }
        }

        return $relatedEntities;
    }

    /**
     * Создает или находит карту с использованием кешированных данных
     */
    private function createOrFindCardWithCachedData($dealFields) {
        $cardNumber = $dealFields['UF_CRM_1756712343'] ?? '';
        if (empty($cardNumber)) {
            return null;
        }

        // Ищем карту в кешированных данных
        $cachedCard = $this->findCardInCachedData($cardNumber);
        
        try {
            // Ищем существующую карту в Bitrix
            $factory = Service\Container::getInstance()->getFactory(1038);
            if (!$factory) {
                return null;
            }

            $items = $factory->getItems([
                'filter' => [
                    '=UF_CRM_3_1759320971349' => $cardNumber
                ],
                'limit' => 1
            ]);

            if (!empty($items)) {
                return $items[0]->getId();
            }

            // Создаем новую карту с данными из кеша
            $clientId = $this->createOrFindClientWithCachedData($cachedCard);
            
            $cardFields = [
                'TITLE' => 'Карта ' . $cardNumber,
                'UF_CRM_3_1759320971349' => $cardNumber,
                'UF_CRM_3_CLIENT' => $clientId,
                'UF_CRM_3_1759315419431' => $cachedCard['is_blocked'] ?? 0,
                'UF_CRM_3_1760598978' => $cachedCard['client'] ?? $cardNumber,
                'UF_CRM_3_1759317288635' => $this->dateManager->formatDate($cachedCard['application_date'] ?? $dealFields['UF_CRM_1760529583'] ?? ''),
                'UF_CRM_3_1760598832' => $cachedCard['warehouse_code'] ?? $dealFields['UF_CRM_1756713651'] ?? '',
                'UF_CRM_3_1760598956' => $cachedCard['discount_card_type'] ?? 'STANDARD'
            ];

            $cardId = $this->createSp($cardFields, 1038);
            
            if ($cardId) {
                $this->logger->logSuccess('card', $cardNumber, $cardId, [
                    'card_number' => $cardNumber,
                    'client_id' => $clientId,
                    'cached_data' => $cachedCard
                ]);
            }

            return $cardId;

        } catch (Exception $e) {
            $this->logger->logGeneralError('card', $cardNumber, "Ошибка создания карты: " . $e->getMessage(), [
                'deal_fields' => $dealFields,
                'cached_card' => $cachedCard
            ]);
            return null;
        }
    }

    /**
     * Создает или находит клиента с использованием кешированных данных
     */
    private function createOrFindClientWithCachedData($cardData) {
        $clientCode = $cardData['client'] ?? '';
        if (empty($clientCode)) {
            return null;
        }

        // Ищем клиента в кешированных данных
        $cachedClient = $this->findClientInCachedData($clientCode);
        
        try {
            print_r('cachedClient' . $cachedClient['phone']);
            // Ищем существующего клиента
            $factory = Service\Container::getInstance()->getFactory(\CCrmOwnerType::Contact);
            if (!$factory) {
                print_r('lol');
                return null;
            }

            $items = $factory->getItems([
                'filter' => [
                    '=UF_CRM_1760599281' => $clientCode
                ],
                'limit' => 1
            ]);

            if (!empty($items)) {
                return $items[0]->getId();
            }

            // Создаем нового клиента с данными из кеша
            $clientFields = [
                'NAME' => $cachedClient['name'] ?? 'Клиент по карте ' . $clientCode,
                'LAST_NAME' => $cachedClient['last_name'] ?? '',
                'SECOND_NAME' => $cachedClient['second_name'] ?? '',
                'UF_CRM_1760599281' => $clientCode,
                'OPENED' => 'Y',
                'TYPE_ID' => 'CLIENT',
                'SOURCE_ID' => 'STORE',
                'PHONE' => !empty($cachedClient['phone']) ? [['VALUE' => $cachedClient['phone'], 'VALUE_TYPE' => 'WORK']] : [],
                'EMAIL' => !empty($cachedClient['email']) ? [['VALUE' => $cachedClient['email'], 'VALUE_TYPE' => 'WORK']] : []
            ];

            $clientId = $this->createContact($clientFields);
            
            if ($clientId) {
                $this->logger->logSuccess('client', $clientCode, $clientId, [
                    'client_code' => $clientCode,
                    'cached_data' => $cachedClient
                ]);
            }

            return $clientId;

        } catch (Exception $e) {
            $this->logger->logGeneralError('client', $clientCode, "Ошибка создания клиента: " . $e->getMessage(), [
                'card_data' => $cardData,
                'cached_client' => $cachedClient
            ]);
            return null;
        }
    }

    /**
     * Создает или находит склад с использованием кешированных данных
     */
    private function createOrFindWarehouseWithCachedData($dealFields) {
        $warehouseCode = $dealFields['UF_CRM_1756713651'] ?? '';
        if (empty($warehouseCode)) {
            return null;
        }

        // Ищем склад в кешированных данных
        $cachedWarehouse = $this->findWarehouseInCachedData($warehouseCode);
        
        try {
            // Ищем существующий склад
            $factory = Service\Container::getInstance()->getFactory(1044);
            if (!$factory) {
                return null;
            }

            $items = $factory->getItems([
                'filter' => [
                    '=UF_CRM_4_CODE' => $warehouseCode
                ],
                'limit' => 1
            ]);

            if (!empty($items)) {
                return $items[0]->getId();
            }

            // Создаем новый склад с данными из кеша
            $warehouseFields = [
                'TITLE' => $cachedWarehouse['name'] ?? 'Магазин ' . $warehouseCode,
                'UF_CRM_4_CODE' => $warehouseCode,
                'UF_CRM_1756713651' => $warehouseCode,
                'UF_CRM_4_ADDRESS' => $cachedWarehouse['address'] ?? '',
                'UF_CRM_4_PHONE' => $cachedWarehouse['phone'] ?? ''
            ];

            $warehouseId = $this->createSp($warehouseFields, 1044);
            
            if ($warehouseId) {
                $this->logger->logSuccess('warehouse', $warehouseCode, $warehouseId, [
                    'warehouse_code' => $warehouseCode,
                    'cached_data' => $cachedWarehouse
                ]);
            }

            return $warehouseId;

        } catch (Exception $e) {
            $this->logger->logGeneralError('warehouse', $warehouseCode, "Ошибка создания склада: " . $e->getMessage(), [
                'deal_fields' => $dealFields,
                'cached_warehouse' => $cachedWarehouse
            ]);
            return null;
        }
    }

    /**
     * Создает или находит товар с использованием кешированных данных
     */
    private function createOrFindProductWithCachedData($dealFields) {
        $itemName = $dealFields['UF_CRM_1759317764974'] ?? '';
        if (empty($itemName)) {
            return null;
        }

        // Ищем товар в кешированных данных
        $cachedItem = $this->findItemInCachedData($itemName);
        try {
            // Ищем существующий товар
            $result = CRest::call('catalog.product.list', [
                'filter' => [
                    '=property65' => $itemName,
                    'iblockId' => 14
                ],
                'select' => ['id', 'name', 'property65', 'iblockId'],
                'limit' => 1
            ]);
            if (isset($result['result']['products']) && !empty($result['result']['products'])) {
                return $result['result']['products'][0]['id'];
            }

            // Создаем новый товар с данными из кеша
            if ($cachedItem) {
                $productFields = $this->prepareProductFieldsFromCachedData($cachedItem, $dealFields['UF_CRM_1759317801939']);
                $result = $this->makeBitrixRequest($productFields);
                
                if ($result && isset($result['result']) && $result['result'] > 0) {
                    $this->logger->logSuccess('product', $itemName, $result['result'], [
                        'item_name' => $itemName,
                        'cached_data' => $cachedItem
                    ]);
                    return $result['result'];
                }
            }

            return null;

        } catch (Exception $e) {
            $this->logger->logGeneralError('product', $itemName, "Ошибка создания товара: " . $e->getMessage(), [
                'deal_fields' => $dealFields,
                'cached_item' => $cachedItem
            ]);
            return null;
        }
    }

    private function findBrandNameByCode($brandCode) {
        try {
            $factory = Service\Container::getInstance()->getFactory(1052);

            if (!$factory) {
                return null;
            }

            $items = $factory->getItems([
                'filter' => [
                    'UF_CRM_6_CODE' => $brandCode
                ],
                'limit' => 1
            ]);

            if (!empty($items)) {
                $brand = $items[0];

                return $brand->getTitle(); // Возвращаем название бренда
            }
            
            return null;
            
        } catch (Exception $e) {
            error_log("Ошибка при поиске названия бренда: " . $e->getMessage());
            return null;
        }
    }
    /**
     * Подготавливает поля товара из кешированных данных
     */
    private function prepareProductFieldsFromCachedData($cachedItem, $weight) {
        // Сначала создаем/проверяем бренд
        $brandId = null;
        $brandName = '';
        
        if (!empty($cachedItem["brand_code"])) {
            // Пытаемся найти название бренда в Bitrix
            $brandName = $this->findBrandNameByCode($cachedItem["brand_code"]);
            
            // Если не нашли в Bitrix, используем данные из кэша
            if (empty($brandName)) {
                $brandName = $cachedItem["brand_name"] ?? $cachedItem["brand_code"];
            }
            
            $brandId = $this->brandManager->createBrandIfNotExists($brandName, $cachedItem["brand_code"]);
        }
        
        // Обрабатываем изображения
        $detailPicture = null;
        if (!empty($cachedItem["product_image_filename"])) {
            $mediaConfig = getMediaConfig();
            $imageUrl = $mediaConfig['base_url'] . $mediaConfig['photos_path'] . $cachedItem["product_image_filename"];
            $detailPicture = $this->processItemImage($imageUrl, $cachedItem["code"] ?? 'unknown');
        }
        print_r($imageUrl);
        print_r($cachedItem);
        $nimPhoto1 = null;
        if (!empty($cachedItem["nim_photo1"])) {
            $mediaConfig = getMediaConfig();
            $imageUrl = $mediaConfig['base_url'] . $mediaConfig['photos_path'] . $cachedItem["nim_photo1"];
            $nimPhoto1 = $this->processItemImage($imageUrl, $cachedItem["code"] ?? 'unknown');
        }
        print_r($imageUrl);
        print_r($cachedItem);
        $productName = $cachedItem["product_name"] ?? 'Товар без названия';
        // Добавляем вес к названию, если он указан
        if (!empty($weight)) {
            $productName .= ' ' . $weight . ' гр. ';
        }
        // Добавляем бренд к названию, если он есть
        if (!empty($brandName)) {
            $productName .= $brandName;
        }

        $fields = [
            'property64' => $cachedItem["code"] ?? '',
            'property65' => $cachedItem["name"] ?? '',
            'property66' => $cachedItem["uin"] ?? '',
            'name' => $productName ?? 'Товар без названия',
            'property67' => $cachedItem["product_sku"] ?? '',
            'property68' => $cachedItem["nim_photo1"] ?? '',
            'property79' => $cachedItem["product_image_filename"] ?? '',
            'property78' => $cachedItem["brand_code"] ?? '',
            'property70' => $cachedItem["size_name"] ?? '',
            'property71' => $cachedItem["metal_name"] ?? '',
            'property72' => $cachedItem["fineness_name"] ?? '',
            'property73' => $cachedItem["feature_name"] ?? '',
            'property85' => $weight ?? 0, // Используем переданный вес
            'iblockId' => 14,
            'iblockSectionId' => 13,
            'active' => 'Y'
        ];
        
        if($detailPicture || $nimPhoto1){
            $fields['detailPicture'] = $detailPicture ?? $nimPhoto1;
        }
        // Добавляем ID бренда если найден
        if ($brandId) {
            $fields['property77'] = $brandId;
        }
        
        return $fields;
    }

    public function mergeDealsAfterCreation() {
        $merger = new DealMerger($this->logger);
        return $merger->mergeDuplicateDeals();
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
        // Сначала создаем/находим связанные сущности
        $relatedEntities = $this->createOrFindRelatedEntities($entityFields);
                $todayMinusThreeDays = date('Y-m-d', strtotime('-3 days'));
                $stageId = "NEW";
                if ($entityFields["date"] > $todayMinusThreeDays) {
                    // Если да, устанавливаем STAGE_ID в "WON"
                    $stageId = "WON";
                }

        // Обновляем поля сделки с ID связанных сущностей
        $entityFields = $this->updateDealFieldsWithRelations($entityFields, $relatedEntities);

        // Создаем сделку
        $entityObject = new \CCrmDeal(false);

        $entityId = $entityObject->Add(
            $entityFields,
            true, // $bUpdateSearch
            []    // $arOptions
        );
        $result = CRest::call(
            'bizproc.workflow.start',
            [
                'TEMPLATE_ID' => 81,
                'DOCUMENT_ID' => [
                    'crm',
                    'CCrmDocumentDeal',
                    'DEAL_' . $entityId,
                ],
            ]
        );
        if (!$entityId) {
            if (method_exists($entityObject, 'GetLAST_ERROR')) {
                error_log("Ошибка при создании сделки: " . $entityObject->GetLAST_ERROR());
            } else {
                error_log("Ошибка при создании сделки");
            }
            return false;
        }

        // Привязываем связанные сущности
        $relationManager = new DealRelationManager($this, $this->logger);
        $relations = $relationManager->findAndAttachRelationsToDeal($entityId, $entityFields);

        // Добавляем товар в сделку (если есть)
        if (!empty($entityFields['UF_CRM_1759317764974'])) { // item_name
            $this->findAndAddProductToDeal(
                $entityId, 
                $entityFields['UF_CRM_1759317764974'], // item_name
                abs($entityFields['UF_CRM_1759317788432']), // count
                $entityFields['OPPORTUNITY'] ?? 0, // price
            );
        }

        return $entityId;
    }

    /**
     * Создает или находит клиента
     */
    private function createOrFindClient($dealFields) {
        $cardNumber = $dealFields['UF_CRM_1756712343'] ?? '';
        if (empty($cardNumber)) {
            return null;
        }

        try {
            // Ищем существующего клиента по номеру карты
            $factory = Service\Container::getInstance()->getFactory(\CCrmOwnerType::Contact);
            if (!$factory) {
                return null;
            }

            $items = $factory->getItems([
                'filter' => [
                    '=UF_CRM_1760599281' => $cardNumber
                ],
                'limit' => 1
            ]);

            if (!empty($items)) {
                return $items[0]->getId();
            }

            // Создаем нового клиента
            $clientFields = [
                'NAME' => 'Клиент по карте ' . $cardNumber,
                'LAST_NAME' => '',
                'UF_CRM_1760599281' => $cardNumber,
                'OPENED' => 'Y',
                'TYPE_ID' => 'CLIENT',
                'SOURCE_ID' => 'STORE'
            ];

            $clientId = $this->createContact($clientFields);
            
            if ($clientId) {
                $this->logger->logSuccess('client', $cardNumber, $clientId, [
                    'card_number' => $cardNumber,
                    'deal_fields' => $dealFields
                ]);
            }

            return $clientId;

        } catch (Exception $e) {
            $this->logger->logGeneralError('client', $cardNumber, "Ошибка создания клиента: " . $e->getMessage(), $dealFields);
            return null;
        }
    }

    /**
     * Создает контакт
     */
    public function createContact($contactFields) {

        try {
            print_r('contactFields' . $contactFields);
            print_r($contactFields);
            $contact = new \CCrmContact(false);
            $contactId = $contact->Add($contactFields, true);

            if ($contactId) {
                // ПОИСК И ПРИВЯЗКА СДЕЛОК К КОНТАКТУ
                $this->findAndAttachDealsToContact($contactId, $contactFields);
                $this->startBp($contactId);
                return $contactId;
            } else {
                $error = method_exists($contact, 'GetLAST_ERROR') ? $contact->GetLAST_ERROR() : 'Неизвестная ошибка';
                throw new Exception($error);
            }
        } catch (Exception $e) {
            throw new Exception("Ошибка создания контакта: " . $e->getMessage());
        }
    }
    private function startBp($contactId){
        $result = CRest::call(
            'bizproc.workflow.start',
            [
                'TEMPLATE_ID' => 50,
                'DOCUMENT_ID' => [
                    'crm',
                    'CCrmDocumentContact',
                    'CONTACT_' . $contactId,
                ],
            ]
        );
        echo 'startBp';
        echo '<PRE>';
        print_r($result);
        echo '</PRE>';
        echo 'startBp';
    }
/**
     * Находит и привязывает сделки к контакту
     */
    private function findAndAttachDealsToContact($contactId, $contactFields) {
        try {
            $clientCode = $contactFields['UF_CRM_1760599281'] ?? '';
            if (empty($clientCode)) {
                return;
            }

            echo "🔍 Ищем сделки для клиента с кодом: {$clientCode}\n";
            
            // Ищем сделки по номеру карты (который соответствует коду клиента)
            $deals = $this->findDealsByCardNumber($clientCode);
            
            if (empty($deals)) {
                echo "ℹ️ Не найдено сделок для клиента: {$clientCode}\n";
                return;
            }

            echo "✅ Найдено сделок для привязки: " . count($deals) . "\n";
            
            $attachedCount = 0;
            foreach ($deals as $deal) {
                $result = $this->attachDealToContact($deal['ID'], $contactId);
                if ($result) {
                    $attachedCount++;
                    echo "  ✅ Привязана сделка ID: {$deal['ID']}\n";
                } else {
                    echo "  ❌ Ошибка привязки сделки ID: {$deal['ID']}\n";
                }
            }

            $this->logger->logSuccess('contact_deals', $contactId, "Привязано сделок: {$attachedCount}", [
                'contact_id' => $contactId,
                'client_code' => $clientCode,
                'total_deals_found' => count($deals),
                'deals_attached' => $attachedCount
            ]);

            echo "🎯 Итог: привязано {$attachedCount} из " . count($deals) . " сделок к контакту {$contactId}\n";

        } catch (Exception $e) {
            $this->logger->logGeneralError('contact_deals', $contactId, "Ошибка привязки сделок: " . $e->getMessage(), [
                'contact_fields' => $contactFields
            ]);
            echo "❌ Ошибка при поиске/привязке сделок: " . $e->getMessage() . "\n";
        }
    }

    /**
     * Ищет сделки по номеру карты (коду клиента)
     */
    private function findDealsByCardNumber($cardNumber) {
        try {
            $deals = DealTable::getList([
                'filter' => [
                    '=UF_CRM_1761200496' => $cardNumber, // Поле с номером карты
                    '=CONTACT_ID' => null // Только сделки без привязанного контакта
                ],
                'select' => ['ID', 'TITLE', 'UF_CRM_1761200496', 'OPPORTUNITY'],
                'order' => ['ID' => 'DESC'],
                'limit' => 100 // Ограничиваем количество для безопасности
            ])->fetchAll();

            return $deals;

        } catch (Exception $e) {
            error_log("Ошибка при поиске сделок по номеру карты {$cardNumber}: " . $e->getMessage());
            return [];
        }
    }
/**
     * Привязывает сделку к контакту
     */
    private function attachDealToContact($dealId, $contactId) {
        try {
            $deal = new \CCrmDeal(false);
            $updateFields = [
                'CONTACT_ID' => $contactId
            ];
            
            $result = $deal->Update($dealId, $updateFields);
            
            if ($result) {
                $this->logger->logSuccess('deal_contact_attach', $dealId, "Сделка привязана к контакту", [
                    'deal_id' => $dealId,
                    'contact_id' => $contactId
                ]);
                return true;
            } else {
                $error = method_exists($deal, 'GetLAST_ERROR') ? $deal->GetLAST_ERROR() : 'Неизвестная ошибка';
                $this->logger->logGeneralError('deal_contact_attach', $dealId, "Ошибка привязки: " . $error, [
                    'deal_id' => $dealId,
                    'contact_id' => $contactId
                ]);
                return false;
            }
            
        } catch (Exception $e) {
            $this->logger->logGeneralError('deal_contact_attach', $dealId, "Исключение при привязке: " . $e->getMessage(), [
                'deal_id' => $dealId,
                'contact_id' => $contactId
            ]);
            return false;
        }
    }

    /**
     * Создает или находит карту лояльности
     */
    private function createOrFindCard($dealFields) {
        $cardNumber = $dealFields['UF_CRM_1756712343'] ?? '';
        if (empty($cardNumber)) {
            return null;
        }

        try {
            // Ищем существующую карту
            $factory = Service\Container::getInstance()->getFactory(1038); // Смарт-процесс карт
            if (!$factory) {
                return null;
            }

            $items = $factory->getItems([
                'filter' => [
                    '=UF_CRM_3_1759320971349' => $cardNumber
                ],
                'limit' => 1
            ]);

            if (!empty($items)) {
                return $items[0]->getId();
            }

            // Создаем новую карту
            $clientId = $this->createOrFindClient($dealFields);
            
            $cardFields = [
                'TITLE' => $cardNumber,
                'UF_CRM_3_1759320971349' => $cardNumber,
                'UF_CRM_3_CLIENT' => $clientId,
                'UF_CRM_3_1759315419431' => 0, // не заблокирована
                'UF_CRM_3_1760598978' => $cardNumber,
                'UF_CRM_3_1759317288635' => $this->dateManager->formatDate($dealFields['UF_CRM_1760529583'] ?? ''),
                'UF_CRM_3_1760598832' => $dealFields['UF_CRM_1756713651'] ?? '', // warehouse_code
                'UF_CRM_3_1760598956' => 'STANDARD' // тип карты
            ];

            $cardId = $this->createSp($cardFields, 1038);
            
            if ($cardId) {
                $this->logger->logSuccess('card', $cardNumber, $cardId, [
                    'card_number' => $cardNumber,
                    'client_id' => $clientId,
                    'deal_fields' => $dealFields
                ]);
            }

            return $cardId;

        } catch (Exception $e) {
            $this->logger->logGeneralError('card', $cardNumber, "Ошибка создания карты: " . $e->getMessage(), $dealFields);
            return null;
        }
    }

    /**
     * Создает или находит склад/магазин
     */
    private function createOrFindWarehouse($dealFields) {
        $warehouseCode = $dealFields['UF_CRM_1756713651'] ?? '';
        if (empty($warehouseCode)) {
            return null;
        }

        try {
            // Ищем существующий склад
            $factory = Service\Container::getInstance()->getFactory(1044); // Смарт-процесс складов
            if (!$factory) {
                return null;
            }

            $items = $factory->getItems([
                'filter' => [
                    '=UF_CRM_4_CODE' => $warehouseCode
                ],
                'limit' => 1
            ]);

            if (!empty($items)) {
                return $items[0]->getId();
            }

            // Создаем новый склад
            $warehouseFields = [
                'TITLE' => $warehouseCode,
                'UF_CRM_4_CODE' => $warehouseCode,
                'UF_CRM_1756713651' => $warehouseCode
            ];

            $warehouseId = $this->createSp($warehouseFields, 1044);
            
            if ($warehouseId) {
                $this->logger->logSuccess('warehouse', $warehouseCode, $warehouseId, [
                    'warehouse_code' => $warehouseCode,
                    'deal_fields' => $dealFields
                ]);
            }

            return $warehouseId;

        } catch (Exception $e) {
            $this->logger->logGeneralError('warehouse', $warehouseCode, "Ошибка создания склада: " . $e->getMessage(), $dealFields);
            return null;
        }
    }

    /**
     * Создает или находит товар
     */
    private function createOrFindProduct($dealFields) {
        $itemName = $dealFields['UF_CRM_1759317764974'] ?? '';
        if (empty($itemName)) {
            return null;
        }

        try {
            // Ищем существующий товар
            $result = CRest::call('catalog.product.list', [
                'filter' => [
                    '=property65' => $itemName,
                    'iblockId' => 14
                ],
                'select' => ['id', 'name', 'property65'],
                'limit' => 1
            ]);

            if (isset($result['result']['products']) && !empty($result['result']['products'])) {
                return $result['result']['products'][0]['id'];
            }

            // Создаем новый товар
            $productFields = [
                'name' => $itemName,
                'property65' => $itemName,
                'property67' => $dealFields['UF_CRM_1759317764974'] ?? '', // SKU
                'property70' => abs($dealFields['UF_CRM_1759317801939']) ?? '', // weight
                'iblockId' => 14,
                'iblockSectionId' => 13,
                'active' => 'Y'
            ];

            $result = $this->makeBitrixRequest($productFields);
            
            if ($result && isset($result['result']) && $result['result'] > 0) {
                $this->logger->logSuccess('product', $itemName, $result['result'], [
                    'item_name' => $itemName,
                    'deal_fields' => $dealFields
                ]);
                return $result['result'];
            }

            return null;

        } catch (Exception $e) {
            $this->logger->logGeneralError('product', $itemName, "Ошибка создания товара: " . $e->getMessage(), $dealFields);
            return null;
        }
    }

    /**
     * Обновляет поля сделки с ID связанных сущностей
     */
    private function updateDealFieldsWithRelations($dealFields, $relatedEntities) {
        if (isset($relatedEntities['client_id'])) {
            $dealFields['CONTACT_ID'] = $relatedEntities['client_id'];
        }
        
        if (isset($relatedEntities['card_id'])) {
            $dealFields['UF_CRM_1756712343'] = $relatedEntities['card_id'];
        }
        
        if (isset($relatedEntities['warehouse_id'])) {
            $dealFields['UF_CRM_1756713651'] = $relatedEntities['warehouse_id'];
        }

        return $dealFields;
    }
/**
 * Находит товар по свойству 65 (название) и добавляет его в сделку
 */
public function findAndAddProductToDeal($dealId, $itemName, $count, $price) {
        try {
            // Ищем товар по свойству 65 (название)
            $product = $this->findProductByProperty65($itemName);
            
            if (!$product) {
                // Создаем временный товар если не найден
                $productId = $this->createOrFindProductByName($itemName);
                
                if (!$productId) {
                    $this->logger->logMappingError('deal_product', $dealId, 'item_name', $itemName, [
                        'deal_id' => $dealId,
                        'item_name' => $itemName,
                        'message' => 'Товар не найден по названию и не удалось создать'
                    ]);
                    return false;
                }
                
                $product = [
                    'id' => $productId,
                    'name' => $itemName
                ];
            }
            
            // Добавляем товар в сделку
            $result = $this->addProductToDeal($dealId, [$product], [$count], [$price]);
            
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
public function addMultipleProductsToDeal($dealId, $products, $counts, $prices) {
        try {
            $productRows = [];
            $totalAmount = 0;
            
            foreach ($products as $index => $product) {
                $count = $counts[$index] ?? 1;
                $price = $prices[$index] ?? 0;
                
                $productRows[] = [
                    'PRODUCT_ID' => $product['id'],
                    'PRODUCT_NAME' => $product['name'],
                    'QUANTITY' => (float)$count,
                    'PRICE' => $price,
                    'TAX_INCLUDED' => 'Y'
                ];
                
                $totalAmount += $price * $count;
            }

            $result = CRest::call('crm.deal.productrows.set', [
                'id' => $dealId,
                'rows' => $productRows
            ]);

            if (isset($result['result']) && $result['result'] === true) {
                // Обновляем общую сумму сделки
                updateDealAmount($dealId, $totalAmount);
                return true;
            }
            
            return false;
            
        } catch (Exception $e) {
            error_log("Ошибка при добавлении товаров в сделку: " . $e->getMessage());
            return false;
        }
    }
public function createOrFindProductByName($itemName) {
    try {
        // Сначала ищем существующий товар
        $product = $this->findProductByProperty65($itemName);
        if ($product) {
            return $product['id'];
        }
        
        // Ищем товар в кешированных данных для получения дополнительной информации
        $cachedItem = $this->findItemInCachedData($itemName);
        
        if ($cachedItem) {
            // Используем данные из кэша для создания товара
            $productFields = $this->prepareProductFieldsFromCachedData($cachedItem, '');
            $result = $this->makeBitrixRequest($productFields);
            
            if ($result && isset($result['result']) && $result['result'] > 0) {
                return $result['result'];
            }
        } else {
            // Создаем новый товар с минимальными данными
            $productFields = [
                'name' => $itemName,
                'property65' => $itemName,
                'iblockId' => 14,
                'iblockSectionId' => 13,
                'active' => 'Y'
            ];

            $result = $this->makeBitrixRequest($productFields);
            
            if ($result && isset($result['result']) && $result['result'] > 0) {
                return $result['result'];
            }
        }

        return null;

    } catch (Exception $e) {
        error_log("Ошибка при создании товара по названию: " . $e->getMessage());
        return null;
    }
}
/**
 * Ищет товар по свойству 65 (название)
 */
public function findProductByProperty65($itemName) {
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
        print_r($fields);
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
        $dateManager = new DateManager();
        switch ($entity) {
            case 'deal':
                // Проверяем наличие пользователя (cashier_code)
                if (!empty($item["cashier_code"])) {
                    $userExists = $this->checkUserExists($item["cashier_code"]);
                    if (!$userExists) {
                        $this->logger->logUserNotFound('deal', $entityId, $item["cashier_code"], $item);
                    }
                }
                $item["title"] = '';
                if ($item["sum"] > 0) {
                    $item["title"] = 'Продажа №' . $item["receipt_number"] . ' от ' . $dateManager->formatDate($item["date"]);
                } elseif ($item["sum"] < 0) {
                    $item["title"] = '';
                }

                $todayMinusThreeDays = date('Y-m-d', strtotime('-3 days'));
                $stageId = "NEW";
                if ($entityFields["date"] > $todayMinusThreeDays) {
                    // Если да, устанавливаем STAGE_ID в "WON"
                    $stageId = "WON";
                }

                $entityFields = [
                    'TITLE' => $item["title"],
                    'OPPORTUNITY' => $item["sum"] ?? 0,
                    'UF_CRM_1761785330' => $item["sum"] ?? 0,
                    'UF_CRM_1756711109104' => $item["receipt_number"] ?? '',
                    'UF_CRM_1756711204935' => $item["register"] ?? '',
                    'UF_CRM_1760529583' => $dateManager->formatDate($item["date"] ?? ''),
                    'UF_CRM_1756713651' => $item["warehouse_code"] ?? '',
                    'UF_CRM_1761200403' => $item["warehouse_code"] ?? '',
                    //'UF_CRM_1759317671' => $item["cashier_code"] ?? '',
                    'UF_CRM_1761200470'  => $item["cashier_code"] ?? '',
                    'UF_CRM_1756712343' => $item["card_number"] ?? '',
                    'UF_CRM_1761200496' => $item["card_number"] ?? '',
                    'UF_CRM_1759321212833' => $item["product_name"] ?? '',
                    'UF_CRM_1759317764974' => $item["item_name"] ?? '',
                    'UF_CRM_1759317788432' => $item["count"] ?? 0,
                    'UF_CRM_1759317801939' => abs($item["weight"]) ?? 0,
                    'STAGE_ID' => $stageId,
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
                    $mediaConfig = getMediaConfig();
                    $imageUrl = $mediaConfig['base_url'] . $mediaConfig['photos_path'] . $item["product_image_filename"];
                    $detailPicture = $this->processItemImage($imageUrl, $entityId);
                }
                
                // Обрабатываем дополнительное фото
                $nimPhoto1 = null;
                if (!empty($item["nim_photo1"])) {
                    $mediaConfig = getMediaConfig();
                    $imageUrl = $mediaConfig['base_url'] . $mediaConfig['photos_path'] . $item["nim_photo1"];
                    $nimPhoto1 = $this->processItemImage($imageUrl, $entityId);
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
    echo "<pre>";
    //print_r($data);
    echo "</pre>";
    $entityManager = new EntityManager(new DateManager(), new ImageProcessor(), $logger);
/*
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
        if ($index >= 0 && $index < 3333) {
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
            
            // СОРТИРОВКА по убыванию (новые сначала)
            $items = $data[$dataKey];
            usort($items, function($a, $b) {
                $dateA = strtotime($a['date'] ?? '');
                $dateB = strtotime($b['date'] ?? '');
                return $dateB <=> $dateA; // по убыванию
            });
            
            foreach ($items as $index => $item) {
                if($index >= 0 && $index < 0){
                    $results[$dataKey][] = $entityManager->addEntity($entityType, $item);
                }
            }
        }
    }
*/
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
function processRecentPurchases($fromDate) {
    $logger = new JsonLogger();
    $entityManager = new EntityManager(new DateManager(), new ImageProcessor(), $logger);
    
    echo "Обработка недавних покупок...\n";
    
    // Получаем только недавние покупки
    $recentPurchases = fetchRecentPurchasesOnly($fromDate);
    
    if (empty($recentPurchases)) {
        echo "Нет покупок за последние 15 дней\n";
        return;
    }
    
    echo "Найдено покупок за последние 15 дней: " . count($recentPurchases) . "\n";
    $purchasesWithItems = [];
    $purchasesWithoutItems = [];
    foreach ($recentPurchases as $purchase) {
        $itemName = $purchase['item_name'] ?? '';
        if (!empty($itemName)) {
            $purchasesWithItems[] = $purchase;
        } else {
            $purchasesWithoutItems[] = $purchase;
        }
    }

    echo "Создаем несуществующие товары...\n";
    $productCreationResults = createMissingProductsFromPurchases($purchasesWithItems, $entityManager, $logger);
    
    echo "Создано товаров: {$productCreationResults['created']}, уже существовало: {$productCreationResults['existing']}\n";
    
    // Группируем покупки по номеру чека и дате
    $groupedPurchases = groupPurchasesByReceipt($purchasesWithItems);
    
    echo "Сгруппировано чеков: " . count($groupedPurchases) . "\n";
    $regularDealsResults['products_created'] = $productCreationResults['created'];
    $regularDealsResults['products_existing'] = $productCreationResults['existing'];

    $results = [
        'created' => [],
        'errors' => [],
        'products_created' => $productCreationResults['created'],
        'products_existing' => $productCreationResults['existing']
    ];
    
    // Создаем сделки для каждой группы покупок (один чек = одна сделка)
    foreach ($groupedPurchases as $receiptKey => $purchasesGroup) {
        $entityId = $purchasesGroup[0]["receipt_number"] ?? 'unknown';
        
        try {
            echo "Создаю сделку для чека: {$entityId} (товаров: " . count($purchasesGroup) . ")\n";

            // Создаем сделку со всеми товарами одним запросом
            $dealId = createDealWithMultipleProducts($purchasesGroup, $entityManager, $logger);
            
            if ($dealId) {
                $results['created'][] = [
                    'receipt_number' => $entityId,
                    'deal_id' => $dealId,
                    'products_count' => count($purchasesGroup)
                ];
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
    $initialBalanceResults = [
        'created' => [],
        'errors' => []
    ];
    
    if (!empty($purchasesWithoutItems)) {
        echo "Создаем сделки начального остатка...\n";
        $initialBalanceResults = createInitialBalanceDeals($purchasesWithoutItems);
    }

    return null;
}
/**
 * Создает все несуществующие товары из списка покупок
 */
function createMissingProductsFromPurchases($purchases, $entityManager, $logger) {
    $results = [
        'created' => 0,
        'existing' => 0,
        'errors' => 0
    ];
    
    // Собираем уникальные названия товаров с информацией о весе
    $uniqueProducts = [];
    foreach ($purchases as $purchase) {
        $itemName = $purchase['item_name'] ?? '';
        $weight = abs($purchase['weight']) ?? 0;
        
        if (!empty($itemName)) {
            if (!isset($uniqueProducts[$itemName])) {
                $uniqueProducts[$itemName] = [
                    'name' => $itemName,
                    'weight' => $weight,
                    'count' => 1
                ];
            } else {
                $uniqueProducts[$itemName]['count']++;
            }
        }
    }
    
    echo "Найдено уникальных товаров: " . count($uniqueProducts) . "\n";
    
    // Создаем несуществующие товары с учетом веса
    foreach ($uniqueProducts as $productData) {
        $productName = $productData['name'];
        $weight = abs($productData['weight']);
        
        try {
            // Проверяем существование товара
            $existingProduct = $entityManager->findProductByProperty65($productName);
            
            if (!$existingProduct) {
                // Создаем новый товар с учетом веса
                $productId = $entityManager->createOrFindProductByNameWithWeight($productName, $weight);
                
                if ($productId) {
                    $results['created']++;
                    echo "✅ Создан товар: {$productName} (ID: {$productId}, вес: {$weight})\n";
                    
                    $logger->logSuccess('product', $productName, $productId, [
                        'action' => 'created_from_purchase',
                        'product_name' => $productName,
                        'weight' => $weight,
                        'purchase_count' => $productData['count']
                    ]);
                } else {
                    $results['errors']++;
                    echo "❌ Ошибка создания товара: {$productName}\n";
                    
                    $logger->logGeneralError('product', $productName, "Ошибка создания товара из покупки", [
                        'product_name' => $productName,
                        'weight' => $weight
                    ]);
                }
            } else {
                $results['existing']++;
                echo "➡️  Товар уже существует: {$productName} (ID: {$existingProduct['id']})\n";
                
                // Обновляем вес существующего товара, если он не установлен
                //$this->updateProductWeightIfNeeded($existingProduct['id'], $weight, $entityManager, $logger);
            }
            
        } catch (Exception $e) {
            $results['errors']++;
            echo "❌ Исключение при создании товара {$productName}: " . $e->getMessage() . "\n";
            
            $logger->logGeneralError('product', $productName, "Исключение при создании товара: " . $e->getMessage(), [
                'product_name' => $productName,
                'weight' => $weight
            ]);
        }
    }
    
    return $results;
}

function groupPurchasesByReceipt($purchases) {
    $grouped = [];
    
    foreach ($purchases as $purchase) {
        $receiptNumber = $purchase['receipt_number'] ?? '';
        $date = $purchase['date'] ?? '';
        
        if (empty($receiptNumber) || empty($date)) {
            continue;
        }
        
        // Создаем ключ группировки: номер чека + дата
        $key = $receiptNumber . '_' . $date;
        
        if (!isset($grouped[$key])) {
            $grouped[$key] = [];
        }
        
        $grouped[$key][] = $purchase;
    }
    
    return $grouped;
}

/**
 * Добавляет все товары из группы в сделку
 */
function addAllProductsToDeal($dealId, $purchasesGroup, $entityManager, $logger) {
    $totalAmount = 0;
    $productsAdded = 0;
    
    foreach ($purchasesGroup as $purchase) {
        $itemName = $purchase['item_name'] ?? '';
        $count = abs($purchase['count']) ?? 1;
        $price = $purchase['sum'] ?? 0;
        $weight = abs($purchase['weight']) ?? 0;
        
        if (empty($itemName)) {
            continue;
        }
        
        try {
            // Добавляем товар в сделку
            $result = $entityManager->findAndAddProductToDeal($dealId, $itemName, $count, $price);
            
            if ($result) {
                $productsAdded++;
                $totalAmount += $price * $count;
            }
            
        } catch (Exception $e) {
            $logger->logGeneralError('deal_product', $dealId, "Ошибка добавления товара в сделку: " . $e->getMessage(), [
                'deal_id' => $dealId,
                'item_name' => $itemName,
                'count' => $count,
                'price' => $price
            ]);
        }
    }
    
    // Обновляем общую сумму сделки
    if ($totalAmount > 0) {
        updateDealAmount($dealId, $totalAmount, $entityManager, $logger);
    }
    
    echo "В сделку {$dealId} добавлено товаров: {$productsAdded} на сумму: {$totalAmount}\n";
    
    return $productsAdded;
}

/**
 * Обновляет общую сумму сделки
 */
function updateDealAmount($dealId, $totalAmount) {
    try {
        $deal = new \CCrmDeal(false);
        $updateFields = [
            'OPPORTUNITY' => $totalAmount,
            'UF_CRM_1761785330' => $totalAmount
        ];
        
        $result = $deal->Update($dealId, $updateFields);
        
        if ($result) {

        } else {

        }
        
    } catch (Exception $e) {

    }
}
function addProductToDeal($dealId, $products, $counts, $prices) {
        return addMultipleProductsToDeal($dealId, $products, $counts, $prices);
    }
function createDealWithMultipleProducts($purchasesGroup, $entityManager, $logger) {
    if (empty($purchasesGroup)) {
        return false;
    }
    $entityManager = new EntityManager(new DateManager(), new ImageProcessor(), $logger);
    $dateManager = new DateManager();
    $firstPurchase = $purchasesGroup[0];
    $entityId = $firstPurchase["receipt_number"] ?? 'unknown';
                $firstPurchase["title"] = '';
                if ((int)$firstPurchase["sum"] > 0) {
                    $firstPurchase["title"] = 'Продажа №' . $firstPurchase["receipt_number"] . ' от ' . $dateManager->formatDate($firstPurchase["date"]);
                } elseif ((int)$firstPurchase["sum"] < 0) {
                    $firstPurchase["title"] = 'Возврат №' . $firstPurchase["receipt_number"] . ' от ' . $dateManager->formatDate($firstPurchase["date"]);
                }
                $todayMinusThreeDays = date('Y-m-d', strtotime('-3 days'));
                $stageId = "NEW";
                if ($entityFields["date"] > $todayMinusThreeDays) {
                    // Если да, устанавливаем STAGE_ID в "WON"
                    $stageId = "WON";
                }
    try {
        // Подготавливаем поля для создания сделки
        $entityFields = [
                    'TITLE' => $firstPurchase["title"],
                    'OPPORTUNITY' => $firstPurchase["sum"] ?? 0,
                    'UF_CRM_1761785330' => $firstPurchase["sum"] ?? 0,
                    'UF_CRM_1756711109104' => $firstPurchase["receipt_number"] ?? '',
                    'UF_CRM_1756711204935' => $firstPurchase["register"] ?? '',
                    'UF_CRM_1760529583' => $dateManager->formatDate($firstPurchase["date"] ?? ''),
                    'UF_CRM_1756713651' => $firstPurchase["warehouse_code"] ?? '',
                    'UF_CRM_1761200403' => $firstPurchase["warehouse_code"] ?? '',
                    //'UF_CRM_1759317671' => $firstPurchase["cashier_code"] ?? '',
                    'UF_CRM_1761200470'  => $firstPurchase["cashier_code"] ?? '',
                    'UF_CRM_1756712343' => $firstPurchase["card_number"] ?? '',
                    'UF_CRM_1761200496' => $firstPurchase["card_number"] ?? '',
                    'UF_CRM_1759321212833' => $firstPurchase["product_name"] ?? '',
                    'UF_CRM_1759317764974' => $firstPurchase["item_name"] ?? '',
                    'UF_CRM_1759317788432' => abs($firstPurchase["count"]) ?? 0,
                    'UF_CRM_1759317801939' => abs($firstPurchase["weight"]) ?? 0,
                    'STAGE_ID' => $stageId,
                    'CURRENCY_ID' => 'RUB',
                    'IS_MANUAL_OPPORTUNITY' => 'Y',
        ];

        // Создаем сделку
        $dealId = $entityManager->createDeal($entityFields);
        
        if (!$dealId) {
            $logger->logGeneralError('deal', $entityId, "Ошибка создания сделки для группы товаров", $firstPurchase);
            return false;
        }
        
        // Подготавливаем данные для добавления всех товаров
        $products = [];
        $counts = [];
        $prices = [];
        $totalAmount = 0;
        
        foreach ($purchasesGroup as $purchase) {
            $itemName = $purchase['item_name'] ?? '';
            $count = abs($purchase['count']) ?? 1;
            $price = $purchase['sum'] ?? 0;
            
            if (empty($itemName)) {
                continue;
            }
            
            // Находим или создаем товар
            $product = $entityManager->findProductByProperty65($itemName);
            if (!$product) {
                $productId = $entityManager->createOrFindProductByName($itemName);
                if ($productId) {
                    $product = [
                        'id' => $productId,
                        'name' => $itemName
                    ];
                } else {
                    $logger->logMappingError('deal_product', $dealId, 'item_name', $itemName, $purchase);
                    continue;
                }
            }
            
            $products[] = $product;
            $counts[] = $count;
            $prices[] = $price;
            $totalAmount += $price * $count;
        }

        // Добавляем все товары одним запросом
        if (!empty($products)) {
            $result = $entityManager->addMultipleProductsToDeal($dealId, $products, $counts, $prices);
            
            if ($result) {
                $logger->logSuccess('deal', $entityId, $dealId, [
                    'receipt_number' => $entityId,
                    'products_count' => count($products),
                    'total_amount' => $totalAmount
                ]);
                
                echo "✅ Сделка создана: {$dealId} для чека {$entityId} (товаров: " . count($products) . ", сумма: {$totalAmount})\n";
                return $dealId;
            } else {
                $logger->logGeneralError('deal', $entityId, "Ошибка добавления товаров в сделку", [
                    'deal_id' => $dealId,
                    'products_count' => count($products)
                ]);
            }
        } else {
            $logger->logGeneralError('deal', $entityId, "Нет товаров для добавления в сделку", [
                'deal_id' => $dealId,
                'purchases_count' => count($purchasesGroup)
            ]);
        }
        
        return $dealId;
        
    } catch (Exception $e) {
        $logger->logGeneralError('deal', $entityId, "Исключение при создании сделки с товарами: " . $e->getMessage(), $firstPurchase);
        return false;
    }
}
function filterRecentPurchases($purchases, $fromDate) {
    $recentPurchases = [];

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
        if ($purchaseDate >= $fromDate) {
            $recentPurchases[] = $purchase;
        }
    }

    return $recentPurchases;
}

// Альтернативная версия с оптимизированным запросом только для покупок
function fetchRecentPurchasesOnly($fromDate) {
    $apiConfig = getApiCredentials();
	$api_username = $apiConfig['username'];
	$api_password = $apiConfig['password'];
	$api_base_url = $apiConfig['base_url'];

    $client = new ApiClient($api_username, $api_password, $api_base_url);
    /*
    // Вычисляем дату 3 минуты назад в нужном формате
    $threeMinutesAgo = new DateTime('-15 days');
    $filterDate = $threeMinutesAgo->format('Y-m-d\TH:i:s');
*/
    
    $purchasesResult = $client->makeRequest('purchases', 'GET');

    if ($purchasesResult['success']) {
        $allPurchases = json_decode($purchasesResult['response'], JSON_UNESCAPED_UNICODE);
        // Фильтруем на стороне PHP если API не поддерживает фильтрацию
		return filterRecentPurchases($allPurchases, $fromDate);
    }
    
    return [];
}

function changeAssigned($seller, $dealId) {
	$result = CRest::call('user.get', [
        'select' => ['ID'],
        'filter' => ['UF_USR_1761980389716' => $seller],
    ]);
    $userId = (int)$result['result'][0]['ID'];

    if($userId > 0){
        $result = CRest::call('crm.deal.update', [
            'id' => $dealId,
            'fields' => ['ASSIGNED_BY_ID' => $userId],
        ]);
    }
};
class ClientSyncManager {
    private $entityManager;
    private $logger;
    
    public function __construct(EntityManager $entityManager, JsonLogger $logger = null) {
        $this->entityManager = $entityManager;
        $this->logger = $logger ?: new JsonLogger();
    }

/**
     * Получает все покупки из API
     */
    private function fetchAllPurchasesFromApi() {
        $apiConfig = getApiCredentials();
        $client = new ApiClient(
            $apiConfig['username'] ?? '', 
            $apiConfig['password'] ?? '', 
            $apiConfig['base_url'] ?? ''
        );
        print_r($client);
        $result = $client->makeRequest('purchases', 'GET');
        
        if ($result['success']) {
            return json_decode($result['response'], JSON_UNESCAPED_UNICODE) ?: [];
        }
        
        return [];
    }

    /**
     * Фильтрует покупки по номеру карты
     */
    private function filterPurchasesByCardNumber($purchases, $cardNumber) {
        $filteredPurchases = [];
        print_r($cardNumber);
        foreach ($purchases as $purchase) {
            $purchaseCardNumber = $purchase['card_number'] ?? '';
            if ($purchaseCardNumber === $cardNumber) {
                $filteredPurchases[] = $purchase;
            }
        }
        
        return $filteredPurchases;
    }
    /**
     * Находит и создает все сделки для клиента
     */
    private function findAndCreateDealsForClient($clientId, $clientCode) {
        try {
            $logger = new JsonLogger();
            $entityManager = new EntityManager(new DateManager(), new ImageProcessor(), $logger);
            
            echo "🔍 Ищем покупки для клиента с кодом: {$clientCode}\n";
            $cardNumber = $this->getOrCreateCardForClient($clientId, $clientCode);
            // Получаем все покупки из API
            $allPurchases = $this->fetchAllPurchasesFromApi();

            // Фильтруем покупки по номеру карты (который соответствует коду клиента)
            $clientPurchases = $this->filterPurchasesByCardNumber($allPurchases, $cardNumber);
            print_r($clientPurchases);
            if (empty($clientPurchases)) {
                echo "ℹ️ Не найдено покупок для карты: {$cardNumber}\n";
                return;
            }

            echo "✅ Найдено покупок для создания сделок: " . count($clientPurchases) . "\n";
            $productCreationResults = createMissingProductsFromPurchases($clientPurchases, $entityManager, $logger);
            // Группируем покупки по номеру чека и дате
            $groupedPurchases = groupPurchasesByReceipt($clientPurchases);
            
            echo "📦 Сгруппировано чеков: " . count($groupedPurchases) . "\n";
            
            $createdDeals = 0;
            
            // Создаем сделки для каждой группы покупок
            foreach ($groupedPurchases as $receiptKey => $purchasesGroup) {
                $dealId = $this->createDealForClient($purchasesGroup, $clientId);
                
                if ($dealId) {
                    $createdDeals++;
                    echo "  ✅ Создана сделка: {$dealId} для чека {$receiptKey}\n";
                } else {
                    echo "  ❌ Ошибка создания сделки для чека {$receiptKey}\n";
                }
            }

            $this->logger->logSuccess('client_deals', $clientId, "Создано сделок: {$createdDeals}", [
                'client_id' => $clientId,
                'client_code' => $clientCode,
                'total_purchases' => count($clientPurchases),
                'grouped_receipts' => count($groupedPurchases),
                'deals_created' => $createdDeals
            ]);

            echo "🎯 Итог: создано {$createdDeals} сделок для клиента {$clientId}\n";

        } catch (Exception $e) {
            $this->logger->logGeneralError('client_deals', $clientId, "Ошибка создания сделок для клиента: " . $e->getMessage(), [
                'client_code' => $clientCode
            ]);
            echo "❌ Ошибка при создании сделок для клиента: " . $e->getMessage() . "\n";
        }
    }
/**
 * Получает или создает карту для клиента
 */
private function getOrCreateCardForClient($clientId, $clientCode) {

    try {
        // Сначала ищем существующую карту для этого клиента
        $existingCard = $this->findCardByClientId($clientId);

        if ($existingCard) {
            echo "  ✅ Найдена существующая карта: {$existingCard['number']}\n";
            return $existingCard['number'];
        }
        $apiCard = $this->findCardInApiByClientCode($clientCode);
        $cardNumber = $this->createCardFromApiData($apiCard, $clientId);
        return $cardNumber;
        
    } catch (Exception $e) {
        echo "  ❌ Ошибка создания карты: " . $e->getMessage() . "\n";
        return null;
    }
}
private function findCardInApiByClientCode($clientCode) {
    try {
        $apiConfig = getApiCredentials();
        $client = new ApiClient(
            $apiConfig['username'] ?? '', 
            $apiConfig['password'] ?? '', 
            $apiConfig['base_url'] ?? ''
        );
        
        // Получаем все карты из API
        $result = $client->makeRequest('cards', 'GET');
        
        if ($result['success']) {
            $allCards = json_decode($result['response'], JSON_UNESCAPED_UNICODE) ?: [];
            
            // Ищем карту по коду клиента
            foreach ($allCards as $card) {
                $cardClientCode = $card['client'] ?? '';
                if ($cardClientCode === $clientCode) {
                    echo "  ✅ Найдена соответствующая карта в API для клиента: {$clientCode}\n";
                    return $card;
                }
            }
            
            echo "  ℹ️  Карта для клиента {$clientCode} не найдена в API\n";
        } else {
            echo "  ⚠️  Ошибка при запросе карт из API: " . ($result['error'] ?? 'Неизвестная ошибка') . "\n";
        }
        
        return null;
        
    } catch (Exception $e) {
        echo "  ⚠️  Исключение при поиске карты в API: " . $e->getMessage() . "\n";
        return null;
    }
}

/**
 * Создает карту из данных API
 */
private function createCardFromApiData($apiCardData, $clientId) {
    try {
        $dateManager = new DateManager();
        
        $cardFields = [
            'TITLE' => $apiCardData['number'] ?? 'Карта из API',
            'UF_CRM_3_1759320971349' => $apiCardData['number'] ?? '',
            'UF_CRM_3_CLIENT' => $clientId,
            'UF_CRM_3_1759315419431' => $apiCardData['is_blocked'] ?? 0,
            'UF_CRM_3_1760598978' => $apiCardData['client'] ?? $apiCardData['number'] ?? '',
            'UF_CRM_3_1759317288635' => $dateManager->formatDate($apiCardData['application_date'] ?? ''),
            'UF_CRM_3_1760598832' => $apiCardData['warehouse_code'] ?? '',
            'UF_CRM_3_1760598956' => $apiCardData['discount_card_type'] ?? 'STANDARD'
        ];
        
        $cardId = $this->entityManager->createSp($cardFields, 1038);
        
        if ($cardId) {
            return $apiCardData['number'];
        } else {
            throw new Exception("Не удалось создать карту из API данных");
        }
        
    } catch (Exception $e) {
        $this->logger->logGeneralError('card_api_creation', $clientId, "Ошибка создания карты из API: " . $e->getMessage(), [
            'api_card_data' => $apiCardData
        ]);
        throw $e;
    }
}
/**
 * Ищет карту по ID клиента
 */
private function findCardByClientId($clientId) {
    try {
        $factory = Service\Container::getInstance()->getFactory(1038);
        if (!$factory) {
            return null;
        }
        
        $items = $factory->getItems([
            'filter' => [
                '=UF_CRM_3_CLIENT' => $clientId
            ],
            'limit' => 1
        ]);
        
        if (!empty($items)) {
            $card = $items[0];
            return [
                'id' => $card->getId(),
                'number' => $card->get('UF_CRM_3_1759320971349') ?? ''
            ];
        }
        
        return null;
        
    } catch (Exception $e) {
        error_log("Ошибка при поиске карты по клиенту {$clientId}: " . $e->getMessage());
        return null;
    }
}
    /**
     * Создает сделку для клиента
     */
    private function createDealForClient($purchasesGroup, $clientId) {
        if (empty($purchasesGroup)) {
            return false;
        }

        $firstPurchase = $purchasesGroup[0];
        $entityId = $firstPurchase["receipt_number"] ?? 'unknown';
        
        try {
            // Проверяем, не существует ли уже сделка с таким номером чека
            $existingDeal = $this->findDealByReceiptNumber($entityId);
            if ($existingDeal) {
                echo "  ➡️  Сделка уже существует для чека {$entityId} (ID: {$existingDeal['ID']})\n";
                
                // Если сделка существует, привязываем к ней клиента если еще не привязан
                if (empty($existingDeal['CONTACT_ID'])) {
                    $this->attachDealToContact($existingDeal['ID'], $clientId);
                }
                
                return $existingDeal['ID'];
            }

            // Создаем новую сделку используя существующую функцию
            $dealId = createDealWithMultipleProducts($purchasesGroup, $this->entityManager, $this->logger);
            
            if ($dealId) {
                // Привязываем сделку к клиенту
                $this->attachDealToContact($dealId, $clientId);
                
                $this->logger->logSuccess('client_deal', $entityId, $dealId, [
                    'client_id' => $clientId,
                    'receipt_number' => $entityId,
                    'products_count' => count($purchasesGroup)
                ]);
            }
            
            return $dealId;
            
        } catch (Exception $e) {
            $this->logger->logGeneralError('client_deal', $entityId, "Ошибка создания сделки для клиента: " . $e->getMessage(), [
                'client_id' => $clientId,
                'purchases_group' => $firstPurchase
            ]);
            return false;
        }
    }

    /**
     * Ищет сделку по номеру чека
     */
    private function findDealByReceiptNumber($receiptNumber) {
        try {
            $deals = DealTable::getList([
                'filter' => [
                    '=UF_CRM_1756711109104' => $receiptNumber // Поле с номером чека
                ],
                'select' => ['ID', 'TITLE', 'UF_CRM_1756711109104', 'CONTACT_ID'],
                'limit' => 1
            ])->fetchAll();

            return !empty($deals) ? $deals[0] : null;

        } catch (Exception $e) {
            error_log("Ошибка при поиске сделки по номеру чека {$receiptNumber}: " . $e->getMessage());
            return null;
        }
    }

    /**
     * Привязывает сделку к контакту
     */
    private function attachDealToContact($dealId, $contactId) {
        try {
            $deal = new \CCrmDeal(false);
            $updateFields = [
                'CONTACT_ID' => $contactId
            ];
            
            $result = $deal->Update($dealId, $updateFields);
            
            if ($result) {
                $this->logger->logSuccess('deal_contact_attach', $dealId, "Сделка привязана к контакту", [
                    'deal_id' => $dealId,
                    'contact_id' => $contactId
                ]);
                return true;
            } else {
                $error = method_exists($deal, 'GetLAST_ERROR') ? $deal->GetLAST_ERROR() : 'Неизвестная ошибка';
                $this->logger->logGeneralError('deal_contact_attach', $dealId, "Ошибка привязки: " . $error, [
                    'deal_id' => $dealId,
                    'contact_id' => $contactId
                ]);
                return false;
            }
            
        } catch (Exception $e) {
            $this->logger->logGeneralError('deal_contact_attach', $dealId, "Исключение при привязке: " . $e->getMessage(), [
                'deal_id' => $dealId,
                'contact_id' => $contactId
            ]);
            return false;
        }
    }

    /**
     * Синхронизирует клиентов из API с Bitrix24
     */
    public function syncClientsWithNotifications() {
        $results = [
            'created' => [],
            'updated' => [],
            'errors' => [],
            'cards_processed' => []
        ];
        
        // Получаем данные клиентов из API
        $apiClients = $this->fetchClientsFromApi();
        
        if (empty($apiClients)) {
            $this->logger->logGeneralError('client_sync', 'batch', "Не удалось получить клиентов из API");
            return $results;
        }
        
        echo "Получено клиентов из API: " . count($apiClients) . "\n";
        
        foreach ($apiClients as $clientData) {
            $clientCode = $clientData['code'] ?? 'unknown';
            echo $clientCode;
            if ($clientCode != '00000068901') {
                echo '$clientCode';
                continue;
            }
            try {
                // Синхронизируем клиента
                $syncResult = $this->syncSingleClient($clientData);
                print_r($syncResult);
                if ($syncResult['status'] === 'created') {
                    $results['created'][] = $syncResult;
                    $this->findAndCreateDealsForClient($syncResult['bitrix_id'], $clientCode);
                    echo "✅ Создан клиент: {$clientCode} (ID: {$syncResult['bitrix_id']})\n";
                } elseif ($syncResult['status'] === 'updated') {
                    $results['updated'][] = $syncResult;
                    echo "🔄 Обновлен клиент: {$clientCode} (ID: {$syncResult['bitrix_id']})\n";
                } elseif ($syncResult['status'] === 'no_changes') {
                    echo "➡️  Без изменений: {$clientCode}\n";
                }
                
                // Обрабатываем карты этого клиента
                if (!empty($clientData['cards'])) {
                    $cardResults = $this->syncClientCards($clientData['cards'], $syncResult['bitrix_id']);
                    $results['cards_processed'] = array_merge($results['cards_processed'], $cardResults);
                }
                
            } catch (Exception $e) {
                $errorResult = [
                    'client_code' => $clientCode,
                    'error' => $e->getMessage(),
                    'client_data' => $clientData
                ];
                $results['errors'][] = $errorResult;
                
                $this->logger->logGeneralError('client_sync', $clientCode, "Ошибка синхронизации клиента: " . $e->getMessage(), $clientData);
                echo "❌ Ошибка клиента {$clientCode}: " . $e->getMessage() . "\n";
            }
        }
        
        // Отправляем уведомления об изменениях
        $this->sendSyncNotifications($results);
        
        return $results;
    }

    /**
     * Ищет клиента по коду
     */
    private function findClientByCode($clientCode) {
        try {
            $factory = Service\Container::getInstance()->getFactory(\CCrmOwnerType::Contact);
            if (!$factory) {
                return null;
            }

            $items = $factory->getItems([
                'filter' => [
                    'UF_CRM_1760599281' => $clientCode, //UF_CRM_1760599281
                ],
                "select"=>["ID", "SECOND_NAME", "NAME"],
                'limit' => 1
            ]);

            if (!empty($items)) {
                $client = $items[0];
                return [
                    'id' => $client->getId(),
                    'SECOND_NAME' => $client->getSecondName(),
                    'phone' => $client->getPhone(),
                    'email' => $client->getEmail(),
                    'NAME' => $client->getName()
                ];
            }

            return null;
            
        } catch (Exception $e) {
            error_log("Ошибка при поиске клиента: " . $e->getMessage());
            return null;
        }
    }
    
    /**
     * Обновляет клиента если есть изменения
     */
    private function updateClientIfChanged($existingClient, $newClientData) {
        $changes = $this->detectClientChanges($existingClient, $newClientData);
                print_r($changes);
                print_r($existingClient);
        if (empty($changes)) {
            return [
                'status' => 'no_changes',
                'bitrix_id' => $existingClient['id'],
                'client_code' => $newClientData['code'],
                'changes' => []
            ];
        }
        
        // Обновляем клиента
        $updateFields = $this->prepareClientUpdateFields($newClientData, $changes);

        $result = $this->updateClient($existingClient['id'], $updateFields);
        
        if ($result) {
            $this->logger->logSuccess('client', $newClientData['code'], $existingClient['id'], [
                'action' => 'updated',
                'changes' => $changes,
                'client_data' => $newClientData
            ]);
            
            return [
                'status' => 'updated',
                'bitrix_id' => $existingClient['id'],
                'client_code' => $newClientData['code'],
                'changes' => $changes
            ];
        } else {
            throw new Exception("Не удалось обновить клиента");
        }
    }
    
    /**
     * Определяет изменения в данных клиента
     */
    private function detectClientChanges($existingClient, $newClientData) {
        $changes = [];
        
        // Проверяем имя
        $newName = trim($newClientData['name'] ?? '');
        $existingName = trim($existingClient['name'] ?? '');
        if ($newName !== $existingName) {
            $changes['name'] = ['from' => $existingName, 'to' => $newName];
        }

        $newMiddleName = trim($newClientData['middle_name'] ?? '');
        $existingMiddleName = trim($existingClient['SECOND_NAME'] ?? '');
        if ($newMiddleName !== $existingMiddleName) {
            $changes['middle_name'] = ['from' => $existingMiddleName, 'to' => $newMiddleName];
        }

        // Проверяем телефон
        $newPhone = $this->normalizePhone($newClientData['phone'] ?? '');
        $existingPhone = $this->normalizePhone($existingClient['phone'] ?? '');
        if ($newPhone !== $existingPhone) {
            $changes['phone'] = ['from' => $existingPhone, 'to' => $newPhone];
        }
        
        // Проверяем email
        $newEmail = strtolower(trim($newClientData['email'] ?? ''));
        $existingEmail = strtolower(trim($existingClient['email'] ?? ''));
        if ($newEmail !== $existingEmail) {
            $changes['email'] = ['from' => $existingEmail, 'to' => $newEmail];
        }
        
        return $changes;
    }
    
    /**
     * Нормализует номер телефона
     */
    private function normalizePhone($phone) {
        // Убираем все нецифровые символы кроме +
        $phone = preg_replace('/[^\d+]/', '', $phone);
        return $phone;
    }
    
    /**
     * Подготавливает поля для создания клиента
     */
    private function prepareClientFields($clientData) {
        $dateManager = new DateManager();
        $genderValue = '';
        if ($clientData['gender'] === 'Женский') {
            $genderValue = 41;
        } elseif ($clientData['gender'] === 'Мужской') {
            $genderValue = 40;
        }

        return [
                'NAME' => $clientData['first_name'] ?? 'Клиент по карте ' . $clientData["code"],
                'LAST_NAME' => $clientData['last_name'] ?? '',
                'SECOND_NAME' => $clientData['middle_name'] ?? '',
                'UF_CRM_1760599281' => $clientData["code"],
                'FM' => [//почта, телефон
                    'EMAIL' => [
                        'n0' => ['VALUE' => $clientData["email"], 'VALUE_TYPE' => 'WORK']
                        ],
                        'PHONE' => [
                            'n0' => ['VALUE' => $clientData["mobile_phone"], 'VALUE_TYPE' => 'WORK']
                        ]
                ],/*
                'PHONE' => [
                    [
                        'VALUE' => $clientData['mobile_phone'],
                        'VALUE_TYPE' => 'WORK',
                    ],
                ],
                'EMAIL' => !empty($clientData['email']) ? [['VALUE' => $clientData['email'], 'VALUE_TYPE' => 'WORK']] : []*/
        ];
    }
    
    /**
     * Подготавливает поля для обновления клиента
     */
    private function prepareClientUpdateFields($clientData, $changes) {
        $fields = [];
        
        if (isset($changes['name'])) {
            $fields['NAME'] = $clientData['name'] ?? '';
        }
        
        if (isset($changes['phone'])) {
            $fields['PHONE'] = !empty($clientData['phone']) ? [['VALUE' => $clientData['phone'], 'VALUE_TYPE' => 'WORK']] : [];
        }
        
        if (isset($changes['email'])) {
            $fields['EMAIL'] = !empty($clientData['email']) ? [['VALUE' => $clientData['email'], 'VALUE_TYPE' => 'WORK']] : [];
        }
        
        return $fields;
    }
    
    /**
     * Обновляет клиента
     */
    private function updateClient($clientId, $updateFields) {
        try {
$arMessageFields = array(
    // получатель
    "TO_USER_ID" => 3,
    // отправитель
    "FROM_USER_ID" => 3, 
    // тип уведомления
    "NOTIFY_TYPE" => 'IM_NOTIFY_CONFIRM',
    // текст уведомления на сайте (доступен html и бб-коды)
    "NOTIFY_MESSAGE" => "Приглашаю вас принять участие во встрече “Мгновенные сообщения и уведомления” которая состоится 15.03.2012 в 14:00",

    // массив описывающий кнопки уведомления
    // в вашем модуле yourmodule в классе CYourModuleEvents в методе CYourModuleEventsIMCallback пишем функцию обработку события
    "NOTIFY_BUTTONS" => Array(
        // 1. название кнопки, 2. значение, 3. шаблон кнопки, 4. переход по адресу после нажатия (не обязательный параметр)
        Array('TITLE' => 'Принять', 'VALUE' => 'Y', 'TYPE' => 'accept' /*, 'URL' => 'http://test.ru/?confirm=Y' */),
        Array('TITLE' => 'Отказаться', 'VALUE' => 'N', 'TYPE' => 'cancel' /*, 'URL' => 'http://test.ru/?confirm=N' */),
    ),
    // символьный код шаблона отправки письма, если не задавать отправляется шаблоном уведомлений
    "NOTIFY_EMAIL_TEMPLATE" => "CALENDAR_INVITATION",
);
if(CModule::IncludeModule("im")){
    CIMNotify::Add($arMessageFields);
}

            /*
            $contact = new \CCrmContact(false);
            $result = $contact->Update($clientId, $updateFields, true);
            */
            return null;//$result;
            
        } catch (Exception $e) {
            error_log("Ошибка при обновлении клиента {$clientId}: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Синхронизирует одну карту
     */
    private function syncSingleCard($cardData, $clientId) {
        $cardNumber = $cardData['number'] ?? 'unknown';
        
        // Ищем существующую карту
        $existingCard = $this->findCardByNumber($cardNumber);
        
        if (!$existingCard) {
            // Создаем новую карту
            return $this->createNewCard($cardData, $clientId);
        } else {
            // Обновляем существующую карту
            return $this->updateCardIfChanged($existingCard, $cardData, $clientId);
        }
    }
    
    /**
     * Ищет карту по номеру
     */
    private function findCardByNumber($cardNumber) {
        try {
            $factory = Service\Container::getInstance()->getFactory(1038);
            if (!$factory) {
                return null;
            }
            
            $items = $factory->getItems([
                'filter' => [
                    '=UF_CRM_3_1759320971349' => $cardNumber
                ],
                'limit' => 1
            ]);
            
            if (!empty($items)) {
                $card = $items[0];
                return [
                    'id' => $card->getId(),
                    'data' => $card->getCompatibleData()
                ];
            }
            
            return null;
            
        } catch (Exception $e) {
            error_log("Ошибка при поиске карты: " . $e->getMessage());
            return null;
        }
    }
    
    /**
     * Создает новую карту
     */
    private function createNewCard($cardData, $clientId) {
        $dateManager = new DateManager();
        
        $cardFields = [
            'TITLE' => $cardData['number'],
            'UF_CRM_3_1759320971349' => $cardData['number'],
            'UF_CRM_3_CLIENT' => $clientId,
            'UF_CRM_3_1759315419431' => $cardData['is_blocked'] ?? 0,
            'UF_CRM_3_1760598978' => $cardData['client'] ?? $cardData['number'],
            'UF_CRM_3_1759317288635' => $dateManager->formatDate($cardData['application_date'] ?? ''),
            'UF_CRM_3_1760598832' => $cardData['warehouse_code'] ?? '',
            'UF_CRM_3_1760598956' => $cardData['discount_card_type'] ?? 'STANDARD'
        ];
        
        $cardId = $this->entityManager->createSp($cardFields, 1038);
        
        if ($cardId) {
            return [
                'status' => 'created',
                'card_id' => $cardId,
                'card_number' => $cardData['number'],
                'client_id' => $clientId
            ];
        } else {
            throw new Exception("Не удалось создать карту");
        }
    }
    
    /**
     * Обновляет карту если есть изменения
     */
    private function updateCardIfChanged($existingCard, $newCardData, $clientId) {
        $dateManager = new DateManager();
        
        $updateFields = [];
        $changes = [];
        
        // Проверяем блокировку карты
        $newBlocked = $newCardData['is_blocked'] ?? 0;
        $existingBlocked = $existingCard['data']['UF_CRM_3_1759315419431'] ?? 0;
        if ($newBlocked != $existingBlocked) {
            $updateFields['UF_CRM_3_1759315419431'] = $newBlocked;
            $changes['is_blocked'] = ['from' => $existingBlocked, 'to' => $newBlocked];
        }
        
        // Проверяем тип карты
        $newType = $newCardData['discount_card_type'] ?? 'STANDARD';
        $existingType = $existingCard['data']['UF_CRM_3_1760598956'] ?? 'STANDARD';
        if ($newType !== $existingType) {
            $updateFields['UF_CRM_3_1760598956'] = $newType;
            $changes['discount_card_type'] = ['from' => $existingType, 'to' => $newType];
        }
        
        // Проверяем дату заявки
        $newAppDate = $dateManager->formatDate($newCardData['application_date'] ?? '');
        $existingAppDate = $existingCard['data']['UF_CRM_3_1759317288635'] ?? '';
        if ($newAppDate !== $existingAppDate) {
            $updateFields['UF_CRM_3_1759317288635'] = $newAppDate;
            $changes['application_date'] = ['from' => $existingAppDate, 'to' => $newAppDate];
        }
        
        if (empty($changes)) {
            return [
                'status' => 'no_changes',
                'card_id' => $existingCard['id'],
                'card_number' => $newCardData['number']
            ];
        }
        
        // Обновляем карту
        $result = $this->updateCard($existingCard['id'], $updateFields);
        
        if ($result) {
            return [
                'status' => 'updated',
                'card_id' => $existingCard['id'],
                'card_number' => $newCardData['number'],
                'changes' => $changes
            ];
        } else {
            throw new Exception("Не удалось обновить карту");
        }
    }
    
    /**
     * Обновляет карту
     */
    private function updateCard($cardId, $updateFields) {
        try {
            $factory = Service\Container::getInstance()->getFactory(1038);
            if (!$factory) {
                return false;
            }
            
            $card = $factory->getItem($cardId);
            if (!$card) {
                return false;
            }
            
            $card->setFromCompatibleData($updateFields);
            $operation = $factory->getUpdateOperation($card);
            $operationResult = $operation->launch();
            
            return $operationResult->isSuccess();
            
        } catch (Exception $e) {
            error_log("Ошибка при обновлении карты {$cardId}: " . $e->getMessage());
            return false;
        }
    }
    
    /**
     * Получает клиентов из API
     */
    private function fetchClientsFromApi() {
        $apiConfig = getApiCredentials();
        $client = new ApiClient(
            $apiConfig['username'] ?? '', 
            $apiConfig['password'] ?? '', 
            $apiConfig['base_url'] ?? ''
        );
        
        $result = $client->makeRequest('clients', 'GET');
        
        if ($result['success']) {
            return json_decode($result['response'], JSON_UNESCAPED_UNICODE) ?: [];
        }
        
        return [];
    }
    
    /**
     * Отправляет уведомления о результатах синхронизации
     */
    private function sendSyncNotifications($results) {
        $message = $this->prepareNotificationMessage($results);
        $this->sendBitrixNotification($message);
    }
    
    /**
     * Подготавливает сообщение для уведомления
     */
    private function prepareNotificationMessage($results) {
        $createdCount = count($results['created']);
        $updatedCount = count($results['updated']);
        $errorsCount = count($results['errors']);
        $cardsCount = count($results['cards_processed']);
        
        $message = "🔄 Синхронизация клиентов завершена\n\n";
        $message .= "✅ Создано клиентов: {$createdCount}\n";
        $message .= "🔄 Обновлено клиентов: {$updatedCount}\n";
        $message .= "🃏 Обработано карт: {$cardsCount}\n";
        $message .= "❌ Ошибок: {$errorsCount}\n";
        
        if ($errorsCount > 0) {
            $message .= "\nПоследние ошибки:\n";
            $errorExamples = array_slice($results['errors'], 0, 3);
            foreach ($errorExamples as $error) {
                $message .= "• {$error['client_code']}: {$error['error']}\n";
            }
        }
        
        return $message;
    }

    /**
     * Отправляет уведомление в Bitrix24
     */
    private function sendBitrixNotification($message) {
        try {
            // Отправляем уведомление администратору
            $result = CRest::call('im.notify', [
                'to' => 3, // ID администратора, можно изменить
                'message' => $message,
                'type' => 'SYSTEM'
            ]);
            
        } catch (Exception $e) {
            error_log("Ошибка отправки Bitrix уведомления: " . $e->getMessage());
        }
    }
    /**
     * Получает все карты клиента из API
     */
    private function getAllClientCardsFromApi($clientCode) {
        try {
            $apiConfig = getApiCredentials();
            $client = new ApiClient(
                $apiConfig['username'] ?? '', 
                $apiConfig['password'] ?? '', 
                $apiConfig['base_url'] ?? ''
            );
            
            // Получаем все карты из API
            $result = $client->makeRequest('cards', 'GET');
            
            if ($result['success']) {
                $allCards = json_decode($result['response'], JSON_UNESCAPED_UNICODE) ?: [];
                
                // Фильтруем карты по коду клиента
                $clientCards = [];
                foreach ($allCards as $card) {
                    $cardClientCode = $card['client'] ?? '';
                    if ($cardClientCode === $clientCode) {
                        $clientCards[] = $card;
                    }
                }
                
                echo "  ✅ Найдено карт в API для клиента {$clientCode}: " . count($clientCards) . "\n";
                return $clientCards;
            } else {
                echo "  ⚠️ Ошибка при запросе карт из API: " . ($result['error'] ?? 'Неизвестная ошибка') . "\n";
            }
            
            return [];
            
        } catch (Exception $e) {
            echo "  ⚠️ Исключение при поиске карт в API: " . $e->getMessage() . "\n";
            return [];
        }
    }

    /**
     * Получает или создает ВСЕ карты для клиента
     */
    private function getAllOrCreateCardsForClient($clientId, $clientCode) {
        $allCardNumbers = [];
        
        // Получаем все карты клиента из API
        $apiCards = $this->getAllClientCardsFromApi($clientCode);
        
        if (empty($apiCards)) {
            echo "  ℹ️ Не найдено карт в API для клиента: {$clientCode}\n";
            return $allCardNumbers;
        }
        
        // Обрабатываем каждую карту
        foreach ($apiCards as $apiCard) {
            try {
                $cardNumber = $this->getOrCreateSingleCard($apiCard, $clientId);
                if ($cardNumber) {
                    $allCardNumbers[] = $cardNumber;
                    echo "  ✅ Обработана карта: {$cardNumber}\n";
                }
            } catch (Exception $e) {
                echo "  ❌ Ошибка обработки карты: " . $e->getMessage() . "\n";
                $this->logger->logGeneralError('card_processing', $clientId, "Ошибка обработки карты: " . $e->getMessage(), [
                    'client_code' => $clientCode,
                    'card_data' => $apiCard
                ]);
            }
        }
        
        return $allCardNumbers;
    }

    /**
     * Получает или создает одну карту
     */
    private function getOrCreateSingleCard($apiCardData, $clientId) {
        $cardNumber = $apiCardData['number'] ?? '';
        if (empty($cardNumber)) {
            return null;
        }
        
        // Сначала ищем существующую карту
        $existingCard = $this->findCardByNumber($cardNumber);
        
        if ($existingCard) {
            echo "    ➡️ Найдена существующая карта: {$cardNumber}\n";
            
            // Проверяем, привязана ли карта к правильному клиенту
            $currentClientId = $existingCard['data']['UF_CRM_3_CLIENT'] ?? null;
            if ($currentClientId != $clientId) {
                echo "    🔄 Обновляем привязку карты к клиенту: {$cardNumber}\n";
                $this->updateCardClient($existingCard['id'], $clientId);
            }
            
            return $cardNumber;
        }
        
        // Создаем новую карту
        return $this->createCardFromApiData($apiCardData, $clientId);
    }

    /**
     * Обновляет привязку карты к клиенту
     */
    private function updateCardClient($cardId, $clientId) {
        try {
            $factory = Service\Container::getInstance()->getFactory(1038);
            if (!$factory) {
                return false;
            }
            
            $card = $factory->getItem($cardId);
            if (!$card) {
                return false;
            }
            
            $card->set('UF_CRM_3_CLIENT', $clientId);
            $operation = $factory->getUpdateOperation($card);
            $operationResult = $operation->launch();
            
            return $operationResult->isSuccess();
            
        } catch (Exception $e) {
            error_log("Ошибка при обновлении привязки карты {$cardId}: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Находит и создает сделки для ВСЕХ карт клиента
     */
    private function findAndCreateDealsForAllClientCards($clientId, $clientCode) {
        try {
            $logger = new JsonLogger();
            $entityManager = new EntityManager(new DateManager(), new ImageProcessor(), $logger);
            
            echo "🔍 Ищем покупки для всех карт клиента: {$clientCode}\n";
            
            // Получаем все номера карт клиента
            $allCardNumbers = $this->getAllOrCreateCardsForClient($clientId, $clientCode);
            
            if (empty($allCardNumbers)) {
                echo "ℹ️ Не найдено карт для клиента: {$clientCode}\n";
                return;
            }
            
            echo "✅ Найдено карт для клиента: " . count($allCardNumbers) . "\n";
            
            // Получаем все покупки из API
            $allPurchases = $this->fetchAllPurchasesFromApi();
            
            // Фильтруем покупки по ВСЕМ номерам карт клиента
            $clientPurchases = $this->filterPurchasesByMultipleCardNumbers($allPurchases, $allCardNumbers);
            
            if (empty($clientPurchases)) {
                echo "ℹ️ Не найдено покупок для карт клиента: " . implode(', ', $allCardNumbers) . "\n";
                return;
            }

            echo "✅ Найдено покупок для создания сделок: " . count($clientPurchases) . "\n";
            
            // Создаем товары если их нет
            $productCreationResults = createMissingProductsFromPurchases($clientPurchases, $entityManager, $logger);
            
            // Группируем покупки по номеру чека и дате
            $groupedPurchases = groupPurchasesByReceipt($clientPurchases);
            
            echo "📦 Сгруппировано чеков: " . count($groupedPurchases) . "\n";
            
            $createdDeals = 0;
            
            // Создаем сделки для каждой группы покупок
            foreach ($groupedPurchases as $receiptKey => $purchasesGroup) {
                $dealId = $this->createDealForClient($purchasesGroup, $clientId);
                
                if ($dealId) {
                    $createdDeals++;
                    echo "  ✅ Создана сделка: {$dealId} для чека {$receiptKey}\n";
                } else {
                    echo "  ❌ Ошибка создания сделки для чека {$receiptKey}\n";
                }
            }

            $this->logger->logSuccess('client_deals', $clientId, "Создано сделок: {$createdDeals}", [
                'client_id' => $clientId,
                'client_code' => $clientCode,
                'card_numbers' => $allCardNumbers,
                'total_purchases' => count($clientPurchases),
                'grouped_receipts' => count($groupedPurchases),
                'deals_created' => $createdDeals
            ]);

            echo "🎯 Итог: создано {$createdDeals} сделок для клиента {$clientId} (по " . count($allCardNumbers) . " картам)\n";

        } catch (Exception $e) {
            $this->logger->logGeneralError('client_deals', $clientId, "Ошибка создания сделок для клиента: " . $e->getMessage(), [
                'client_code' => $clientCode
            ]);
            echo "❌ Ошибка при создании сделок для клиента: " . $e->getMessage() . "\n";
        }
    }

    /**
     * Фильтрует покупки по нескольким номерам карт
     */
    private function filterPurchasesByMultipleCardNumbers($purchases, $cardNumbers) {
        $filteredPurchases = [];
        $cardNumbersSet = array_flip($cardNumbers); // Для быстрого поиска
        
        foreach ($purchases as $purchase) {
            $purchaseCardNumber = $purchase['card_number'] ?? '';
            if (isset($cardNumbersSet[$purchaseCardNumber])) {
                $filteredPurchases[] = $purchase;
            }
        }
        
        return $filteredPurchases;
    }

    /**
     * Создает нового клиента со всеми картами
     */
    private function createNewClient($clientData) {
        $clientFields = $this->prepareClientFields($clientData);
        $clientId = $this->entityManager->createContact($clientFields);
        
        if ($clientId) {
            $clientCode = $clientData['code'] ?? 'unknown';
            
            // Создаем сделки для ВСЕХ карт клиента
            $this->findAndCreateDealsForAllClientCards($clientId, $clientCode);
            
            return [
                'status' => 'created',
                'bitrix_id' => $clientId,
                'client_code' => $clientData['code'],
                'changes' => 'new_client'
            ];
        } else {
            throw new Exception("Не удалось создать клиента");
        }
    }

    /**
     * Синхронизирует одного клиента со всеми его картами
     */
    private function syncSingleClient($clientData) {
        $clientCode = $clientData['code'] ?? 'unknown';
        
        // Ищем существующего клиента
        $existingClient = $this->findClientByCode($clientCode);
        
        if (!$existingClient) {
            // Создаем нового клиента со всеми картами
            return $this->createNewClient($clientData);
        } else {
            // Обновляем клиента и синхронизируем все карты
            $syncResult = $this->updateClientIfChanged($existingClient, $clientData);
            
            // После обновления клиента синхронизируем все его карты и сделки
            if ($syncResult['status'] !== 'error') {
                $this->findAndCreateDealsForAllClientCards($existingClient['id'], $clientCode);
            }
            
            return $syncResult;
        }
    }

    /**
     * Получает все существующие карты клиента из Bitrix
     */
    private function getExistingClientCards($clientId) {
        try {
            $factory = Service\Container::getInstance()->getFactory(1038);
            if (!$factory) {
                return [];
            }
            
            $items = $factory->getItems([
                'filter' => [
                    '=UF_CRM_3_CLIENT' => $clientId
                ],
                'select' => ['ID', 'UF_CRM_3_1759320971349']
            ]);
            
            $existingCards = [];
            foreach ($items as $item) {
                $existingCards[] = [
                    'id' => $item->getId(),
                    'number' => $item->get('UF_CRM_3_1759320971349')
                ];
            }
            
            return $existingCards;
            
        } catch (Exception $e) {
            error_log("Ошибка при получении карт клиента {$clientId}: " . $e->getMessage());
            return [];
        }
    }

    /**
     * Синхронизирует карты клиента (создает недостающие, обновляет существующие)
     */
    private function syncClientCards($cardsData, $clientId) {
        $results = [];
        $processedCardNumbers = [];
        
        // Получаем существующие карты клиента
        $existingCards = $this->getExistingClientCards($clientId);
        $existingCardNumbers = array_column($existingCards, 'number');
        
        foreach ($cardsData as $cardData) {
            $cardNumber = $cardData['number'] ?? 'unknown';
            $processedCardNumbers[] = $cardNumber;
            
            try {
                // Проверяем, существует ли уже карта
                if (in_array($cardNumber, $existingCardNumbers)) {
                    // Карта существует - обновляем
                    $cardResult = $this->updateCardIfChanged($this->findCardByNumber($cardNumber), $cardData, $clientId);
                } else {
                    // Карта не существует - создаем
                    $cardResult = $this->createNewCard($cardData, $clientId);
                }
                
                $results[] = $cardResult;
                
                if ($cardResult['status'] === 'created') {
                    echo "  ✅ Создана карта: {$cardResult['card_number']}\n";
                } elseif ($cardResult['status'] === 'updated') {
                    echo "  🔄 Обновлена карта: {$cardResult['card_number']}\n";
                }
                
            } catch (Exception $e) {
                $errorResult = [
                    'status' => 'error',
                    'card_number' => $cardNumber,
                    'error' => $e->getMessage()
                ];
                $results[] = $errorResult;
                
                $this->logger->logGeneralError('card_sync', $cardNumber, "Ошибка синхронизации карты: " . $e->getMessage(), $cardData);
                echo "  ❌ Ошибка карты: " . $e->getMessage() . "\n";
            }
        }
        
        // Логируем неиспользуемые карты (которые есть в Bitrix но нет в API)
        $this->logUnusedCards($existingCardNumbers, $processedCardNumbers, $clientId);
        
        return $results;
    }

    /**
     * Логирует карты которые есть в Bitrix но отсутствуют в API данных
     */
    private function logUnusedCards($existingCardNumbers, $processedCardNumbers, $clientId) {
        $unusedCards = array_diff($existingCardNumbers, $processedCardNumbers);
        
        if (!empty($unusedCards)) {
            $this->logger->logGeneralError('unused_cards', $clientId, "Найдены карты в Bitrix отсутствующие в API", [
                'client_id' => $clientId,
                'unused_cards' => $unusedCards,
                'total_unused' => count($unusedCards)
            ]);
            
            echo "  ⚠️  Найдены карты в Bitrix отсутствующие в API: " . count($unusedCards) . "\n";
            foreach ($unusedCards as $unusedCard) {
                echo "    - {$unusedCard}\n";
            }
        }
    }

    // ... остальные существующие методы класса ClientSyncManager ...
}

/**
 * Поиск товаров по всем картам клиента
 */
function findProductsByAllClientCards($clientCode) {
    $logger = new JsonLogger();
    $entityManager = new EntityManager(new DateManager(), new ImageProcessor(), $logger);
    
    echo "🔍 Поиск товаров по всем картам клиента: {$clientCode}\n";
    
    try {
        // Получаем все карты клиента из API
        $clientSyncManager = new ClientSyncManager($entityManager, $logger);
        $apiCards = $clientSyncManager->getAllClientCardsFromApi($clientCode);
        
        if (empty($apiCards)) {
            echo "ℹ️ Не найдено карт для клиента: {$clientCode}\n";
            return [];
        }
        
        $allCardNumbers = array_column($apiCards, 'number');
        echo "✅ Найдено карт: " . count($allCardNumbers) . "\n";
        
        // Получаем все покупки из API
        $allPurchases = $clientSyncManager->fetchAllPurchasesFromApi();
        
        // Фильтруем покупки по всем картам клиента
        $clientPurchases = $clientSyncManager->filterPurchasesByMultipleCardNumbers($allPurchases, $allCardNumbers);
        
        if (empty($clientPurchases)) {
            echo "ℹ️ Не найдено покупок для карт клиента\n";
            return [];
        }
        
        // Извлекаем уникальные товары
        $uniqueProducts = [];
        foreach ($clientPurchases as $purchase) {
            $itemName = $purchase['item_name'] ?? '';
            if (!empty($itemName)) {
                $uniqueProducts[$itemName] = [
                    'name' => $itemName,
                    'weight' => abs($purchase['weight']) ?? 0,
                    'price' => $purchase['sum'] ?? 0,
                    'count' => ($uniqueProducts[$itemName]['count'] ?? 0) + 1
                ];
            }
        }
        
        echo "✅ Найдено уникальных товаров: " . count($uniqueProducts) . "\n";
        
        return [
            'client_code' => $clientCode,
            'card_count' => count($allCardNumbers),
            'purchase_count' => count($clientPurchases),
            'unique_products' => array_values($uniqueProducts),
            'card_numbers' => $allCardNumbers
        ];
        
    } catch (Exception $e) {
        $logger->logGeneralError('product_search', $clientCode, "Ошибка поиска товаров: " . $e->getMessage());
        echo "❌ Ошибка поиска товаров: " . $e->getMessage() . "\n";
        return [];
    }
}

/**
 * Создает сделки "внесение начального остатка" для покупок без товаров
 */
function createInitialBalanceDeals($purchases) {
    $logger = new JsonLogger();
    $entityManager = new EntityManager(new DateManager(), new ImageProcessor(), $logger);
    $dateManager = new DateManager();
    
    echo "🔍 Ищем покупки без товаров для создания сделок начального остатка...\n";
    
    $results = [
        'created' => [],
        'errors' => []
    ];
    
    foreach ($purchases as $purchase) {
        // Проверяем, что это покупка без товара (пустое item_name)
        $itemName = $purchase['item_name'] ?? '';
        $receiptNumber = $purchase['receipt_number'] ?? '';
        $cardNumber = $purchase['card_number'] ?? '';
        $sum = $purchase['sum'] ?? 0;
        
        if (empty($itemName) && !empty($receiptNumber) && !empty($cardNumber) && $sum != 0) {
            try {
                echo "📝 Обрабатываю покупку без товара: {$receiptNumber}, карта: {$cardNumber}, сумма: {$sum}\n";
                
                // Создаем сделку "внесение начального остатка"
                $dealId = createInitialBalanceDeal($purchase, $entityManager, $dateManager);
                
                if ($dealId) {
                    $results['created'][] = [
                        'receipt_number' => $receiptNumber,
                        'deal_id' => $dealId,
                        'card_number' => $cardNumber,
                        'sum' => $sum
                    ];
                    echo "✅ Создана сделка начального остатка: {$dealId} для карты {$cardNumber}\n";
                } else {
                    $results['errors'][] = [
                        'receipt_number' => $receiptNumber,
                        'error' => 'Ошибка создания сделки'
                    ];
                    echo "❌ Ошибка создания сделки для чека {$receiptNumber}\n";
                }
                
            } catch (Exception $e) {
                $results['errors'][] = [
                    'receipt_number' => $receiptNumber,
                    'error' => $e->getMessage()
                ];
                echo "❌ Исключение при создании сделки для чека {$receiptNumber}: " . $e->getMessage() . "\n";
            }
        }
    }
    
    echo "\n=== РЕЗУЛЬТАТЫ СОЗДАНИЯ СДЕЛОК НАЧАЛЬНОГО ОСТАТКА ===\n";
    echo "Создано сделок: " . count($results['created']) . "\n";
    echo "Ошибок: " . count($results['errors']) . "\n";
    
    // Логируем результаты
    $logger->logGeneralError('initial_balance_deals', 'batch', "Созданы сделки начального остатка", [
        'total_processed' => count($purchases),
        'created' => count($results['created']),
        'errors' => count($results['errors']),
        'results' => $results
    ]);
    
    return $results;
}

/**
 * Создает сделку "внесение начального остатка"
 */
function createInitialBalanceDeal($purchase, $entityManager, $dateManager) {
    $receiptNumber = $purchase['receipt_number'] ?? 'unknown';
    $cardNumber = $purchase['card_number'] ?? '';
    $sum = $purchase['sum'] ?? 0;
    
    // Определяем тип операции по сумме
    $operationType = $sum > 0 ? 'пополнение' : 'списание';
    
    // Создаем название для сделки
    $dealTitle = "Внесение начального остатка по карте {$cardNumber}";
                $todayMinusThreeDays = date('Y-m-d', strtotime('-3 days'));
                $stageId = "NEW";
                if ($entityFields["date"] > $todayMinusThreeDays) {
                    // Если да, устанавливаем STAGE_ID в "WON"
                    $stageId = "WON";
                }
    try {
        // Подготавливаем поля для создания сделки
        $entityFields = [
            'TITLE' => $dealTitle,
            'OPPORTUNITY' => abs($sum), // Используем абсолютное значение суммы
            'UF_CRM_1761785330' => abs($sum),
            'UF_CRM_1756711109104' => $receiptNumber,
            'UF_CRM_1756711204935' => $purchase['register'] ?? '',
            'UF_CRM_1760529583' => $dateManager->formatDate($purchase['date'] ?? ''),
            'UF_CRM_1756713651' => $purchase['warehouse_code'] ?? '',
            'UF_CRM_1761200403' => $purchase['warehouse_code'] ?? '',
            'UF_CRM_1761200470' => $purchase['cashier_code'] ?? '',
            'UF_CRM_1756712343' => $cardNumber,
            'UF_CRM_1761200496' => $cardNumber,
            'STAGE_ID' => $stageId,
            'CURRENCY_ID' => 'RUB',
            'IS_MANUAL_OPPORTUNITY' => 'Y',
            // Добавляем специальное поле для отметки, что это сделка начального остатка
            'UF_CRM_1763617811' => 'Y' // Предполагаемое пользовательское поле
        ];

        // Создаем сделку
        $dealId = $entityManager->createDeal($entityFields);
        
        if ($dealId) {
            // Логируем успешное создание
            $entityManager->getLogger()->logSuccess('initial_balance_deal', $receiptNumber, $dealId, [
                'receipt_number' => $receiptNumber,
                'card_number' => $cardNumber,
                'sum' => $sum,
                'operation_type' => $operationType,
                'deal_title' => $dealTitle
            ]);
            
            return $dealId;
        }
        
        return false;
        
    } catch (Exception $e) {
        $entityManager->getLogger()->logGeneralError('initial_balance_deal', $receiptNumber, "Ошибка создания сделки начального остатка: " . $e->getMessage(), $purchase);
        return false;
    }
}

/**
 * Обработка action=clients
 */
function processClientsSync() {
    $logger = new JsonLogger();
    $entityManager = new EntityManager(new DateManager(), new ImageProcessor(), $logger);
    $clientSyncManager = new ClientSyncManager($entityManager, $logger);
    
    echo "🔄 Начинаем синхронизацию клиентов...\n";
    
    $startTime = microtime(true);
    $results = $clientSyncManager->syncClientsWithNotifications();
    $executionTime = round(microtime(true) - $startTime, 2);
    
    echo "\n=== РЕЗУЛЬТАТЫ СИНХРОНИЗАЦИИ КЛИЕНТОВ ===\n";
    echo "Время выполнения: {$executionTime} сек.\n";
    echo "Создано клиентов: " . count($results['created']) . "\n";
    echo "Обновлено клиентов: " . count($results['updated']) . "\n";
    echo "Ошибок: " . count($results['errors']) . "\n";
    echo "Обработано карт: " . count($results['cards_processed']) . "\n";
    
    // Детальная статистика по картам
    $cardStats = [
        'created' => 0,
        'updated' => 0,
        'no_changes' => 0,
        'errors' => 0
    ];
    
    foreach ($results['cards_processed'] as $cardResult) {
        $status = $cardResult['status'] ?? 'unknown';
        if (isset($cardStats[$status])) {
            $cardStats[$status]++;
        }
    }
    
    echo "\n--- Статистика по картам ---\n";
    echo "Создано карт: {$cardStats['created']}\n";
    echo "Обновлено карт: {$cardStats['updated']}\n";
    echo "Без изменений: {$cardStats['no_changes']}\n";
    echo "Ошибок карт: {$cardStats['errors']}\n";
    
    return $results;
}

if(strpos($_SERVER['REQUEST_URI'], 'action=clients') !== false){
    
} elseif(strpos($_SERVER['REQUEST_URI'], 'action=update') !== false){
    processClientsSync();
    // Проверяем наличие параметра date
    if(isset($_REQUEST['date']) && !empty($_REQUEST['date'])) {
        $timestamp = $_REQUEST['date'];
        $fromDate = new DateTime();
        $fromDate->setTimestamp($timestamp);
        print_r('fromDate');
        print_r($fromDate);
       // processRecentPurchases($fromDate);
    } else {
    }
} elseif(strpos($_SERVER['REQUEST_URI'], 'action=count') !== false){
$currentDate = new DateTime();
$oneYearAgo = (new DateTime())->modify('-1 year');

// Получаем все сделки
$arFilter = array();            
$arSelect = array(
   "ID",
   "UF_CRM_1760529583",
   "OPPORTUNITY",
   "CONTACT_ID",
   "DATE_CREATE"
);        
$arDeals = DealTable::getList([
   'order'=>['ID' => 'DESC'],
   'filter'=>$arFilter,
   'select'=>$arSelect,
])->fetchAll();

$result = [];
$contactsWithDeals = []; // Массив для хранения ID контактов, у которых есть сделки

// Переменные для общих сумм
$totalOpportunityAll = 0;
$totalDealsCountAll = 0;

foreach($arDeals as $deal){
    $contactId = $deal['CONTACT_ID'];
    
    // Добавляем контакт в список контактов со сделками
    $contactsWithDeals[$contactId] = true;
    
    $date = $deal["UF_CRM_1760529583"]->toString();
    $dealDate = new DateTime($date);
    
    // Обновляем общие суммы
    $totalOpportunityAll += $deal['OPPORTUNITY'];
    $totalDealsCountAll++;
    
    if (!isset($result[$contactId])) {
        $result[$contactId] = [
            'CONTACT_ID' => $contactId,
            'TOTAL_OPPORTUNITY' => 0,
            'TOTAL_OPPORTUNITY_YEAR' => 0,
            'DEALS_COUNT' => 0,
            'DEALS_COUNT_YEAR' => 0,
            'LAST_PURCHASE_DATE' => $date
        ];
    }
    
    // Общая сумма покупок
    $result[$contactId]['TOTAL_OPPORTUNITY'] += $deal['OPPORTUNITY'];
    $result[$contactId]['DEALS_COUNT']++;
    
    // Сумма покупок за последний год
    if ($dealDate >= $oneYearAgo) {
        $result[$contactId]['TOTAL_OPPORTUNITY_YEAR'] += $deal['OPPORTUNITY'];
        $result[$contactId]['DEALS_COUNT_YEAR']++;
    }
    
    // Самая поздняя дата покупки
    if (strtotime($date) > strtotime($result[$contactId]['LAST_PURCHASE_DATE'])) {
        $result[$contactId]['LAST_PURCHASE_DATE'] = $date;
    }
}

// Получаем все контакты, чтобы найти тех, у кого нет сделок
$allContacts = CCrmContact::GetListEx(
    [],
    [],
    false,
    false,
    ['ID']
);

$contactsWithoutDeals = [];
while ($contact = $allContacts->Fetch()) {
    $contactId = $contact['ID'];
    // Если контакта нет в массиве контактов со сделками, добавляем его в список контактов без сделок
    if (!isset($contactsWithDeals[$contactId])) {
        $contactsWithoutDeals[$contactId] = [
            'CONTACT_ID' => $contactId,
            'TOTAL_OPPORTUNITY' => 0,
            'TOTAL_OPPORTUNITY_YEAR' => 0,
            'DEALS_COUNT' => 0,
            'DEALS_COUNT_YEAR' => 0,
            'LAST_PURCHASE_DATE' => null
        ];
    }
}

// Объединяем контакты со сделками и без сделок
$allContactsData = $result + $contactsWithoutDeals;

// Форматируем результат и добавляем поле в зависимости от суммы
foreach($allContactsData as &$contactData) {
    // Определяем поле в зависимости от суммы
    $totalOpportunity = $contactData['TOTAL_OPPORTUNITY'];
    if ($totalOpportunity < 1000000) {
        $contactData['SUMM_LIST'] = 55;
    } elseif ($totalOpportunity >= 1000000 && $totalOpportunity < 5000000) {
        $contactData['SUMM_LIST'] = 56;
    } else {
        $contactData['SUMM_LIST'] = 57; // или другое значение по умолчанию для сумм от 3 миллионов и выше
    }

    // Форматируем сумму покупок за год или ставим "-"
    if ($contactData['DEALS_COUNT_YEAR'] > 0) {
        $contactData['TOTAL_OPPORTUNITY_YEAR_FORMATTED'] = number_format(
            floor($contactData['TOTAL_OPPORTUNITY_YEAR']), 
            0, '', ' '
        );
        $contactData['DEALS_COUNT_YEAR_FORMATTED'] = $contactData['DEALS_COUNT_YEAR'];
    } else {
        $contactData['TOTAL_OPPORTUNITY_YEAR_FORMATTED'] = '-';
        $contactData['DEALS_COUNT_YEAR_FORMATTED'] = '-'; // Если посещений 0, ставим "-"
    }
    
    if ($contactData['DEALS_COUNT'] > 0) {
        $contactData['TOTAL_OPPORTUNITY_FORMATTED'] = number_format(
            floor($contactData['TOTAL_OPPORTUNITY']), 
            0, '', ' '
        );
        $contactData['DEALS_COUNT_FORMATTED'] = $contactData['DEALS_COUNT'];
    } else {
        $contactData['TOTAL_OPPORTUNITY_FORMATTED'] = '-';
        $contactData['DEALS_COUNT'] = '-'; // Если посещений 0, ставим "-"
    }
    
    // Форматируем дату последней покупки (убираем время)
    if ($contactData['LAST_PURCHASE_DATE']) {
        $lastPurchaseDate = new DateTime($contactData['LAST_PURCHASE_DATE']);
        $contactData['LAST_PURCHASE_DATE_FORMATTED'] = $lastPurchaseDate->format('d.m.Y');
    } else {
        $contactData['LAST_PURCHASE_DATE_FORMATTED'] = '-'; // Для контактов без сделок
    }
}

$allContactsData = array_values($allContactsData);


// Обновляем все контакты
$oContact = new CCrmContact(false);
foreach($allContactsData as $contactData) {
    $contactId = $contactData['CONTACT_ID'];

    $arFields = array(
        "UF_CRM_1763617810" => $contactData['TOTAL_OPPORTUNITY_YEAR_FORMATTED'], // Сумма покупок за год
        "UF_CRM_1759327062433" => $contactData['DEALS_COUNT_YEAR_FORMATTED'], // Число посещений за год (число сделок)
        "UF_CRM_1763617746" => $contactData['LAST_PURCHASE_DATE_FORMATTED'],
        "UF_CRM_1763645912" => $contactData['TOTAL_OPPORTUNITY_FORMATTED'],
        "UF_CRM_1759327078738" => $contactData['DEALS_COUNT_FORMATTED'],
        "UF_CRM_1759327027801" => $contactData['SUMM_LIST'],
    );

    // Обновляем контакт
    $updateResult = $oContact->Update($contactId, $arFields);
    
    if (!$updateResult) {
        // Обработка ошибки обновления
        echo "Ошибка при обновлении контакта ID: " . $contactId . "<br>";
    }
}

// Выводим статистику для информации
echo "Обработано контактов со сделками: " . count($result) . "<br>";
echo "Обработано контактов без сделок: " . count($contactsWithoutDeals) . "<br>";
echo "Всего обработано контактов: " . count($allContactsData) . "<br>";
echo "<pre>";
print_r($result);
echo "</pre>";
	//changeAssigned($_REQUEST['seller'], $_REQUEST['deal_id']);
} else {
    $result = fetchAllData();

echo "<pre>";
print_r($result);
echo "</pre>";
	//main();
}


//$client = new ApiClient($api_username, $api_password, $api_base_url);
//$itemsResult = $client->makeRequest('clients/changes&message_number=256832', 'DELETE',);

?>