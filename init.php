<?php
use Bitrix\Main\Web\HttpClient;
AddEventHandler('im', 'OnAfterConfirmNotify', 'handleConfirmNotify');

function handleConfirmNotify($notifyId, $userId, $tag, $data) {
    $response = [
        'action' => 'UNKNOWN',
        'status' => 'ERROR',
        'message' => 'Неизвестный тип действия'
    ];
    
    // Определяем действие по тегу
    if (strpos($tag, 'approve_') === 0) {
        // Извлекаем ID контакта из сообщения
        $message = $data['MESSAGE'] ?? '';
        if (preg_match('/контакта #(\d+)/', $message, $matches)) {
            $contactId = $matches[1];
        }
        $changeId = $data['ID'] ?? '';

        $result = approveChange($changeId);
        $response = [
            'action' => 'APPROVE',
            'status' => $result['success'] ? 'SUCCESS' : 'ERROR',
            'message' => $result['message']
        ];

    } elseif (strpos($tag, 'reject_') === 0) {
        $changeId = $data['ID'] ?? '';
        $result = rejectChange($changeId);
        
        $response = [
            'action' => 'REJECT',
            'status' => $result['success'] ? 'SUCCESS' : 'ERROR',
            'message' => $result['message']
        ];
        
    }

    return $response;
}

function findChangeById($changeId) {
    $changes = getChangesList();

    foreach ($changes as $index => $change) {
        if (isset($change['change_id']) && $change['change_id'] == $changeId) {
            return [
                'change' => $change,
                'index' => $index
            ];
        }
    }
    
    return null;
}

function getChangesList() {
    $logFile = __DIR__ . '/modules/timofey.connector1c/logs/changes_tracker.json';
    
    if (!file_exists($logFile)) {
        return [];
    }
    
    $jsonData = file_get_contents($logFile);
    $changes = json_decode($jsonData, true);
    
    return is_array($changes) ? $changes : [];
}

function approveChange($changeId) {
    $change = findChangeById($changeId);

    if (!$change) {
        return [
            'success' => false,
            'message' => 'Изменение не найдено'
        ];
    }
    
    // Обновляем статус изменения
    $statusUpdate = updateChangeStatus($changeId, 'approved');
    
    if (!$statusUpdate['success']) {
        return $statusUpdate;
    }
    
    return [
        'success' => true,
        'message' => 'Изменение успешно подтверждено и применено'
    ];
}

function rejectChange($changeId) {
    $change = findChangeById($changeId);

    if (!$change) {
        return [
            'success' => false,
            'message' => 'Изменение не найдено'
        ];
    }
    
    // Обновляем статус изменения на rejected
    $statusUpdate = updateChangeStatus($changeId, 'rejected');
    
    if (!$statusUpdate['success']) {
        return $statusUpdate;
    }
    
    return [
        'success' => true,
        'message' => 'Изменение успешно отклонено'
    ];
}

function updateChangeStatus($changeId, $status) {
    $changes = getChangesList();
    $change = findChangeById($changeId);

    if (!$change) {
        return [
            'success' => false,
            'message' => 'Изменение с ID ' . $changeId . ' не найдено'
        ];
    }
    
    $index = $change['index'];
    
    // Для статуса approved - применяем изменение к контакту
    if ($status === 'approved') {
        $updateResult = updateContact($change['change']);
        if (!$updateResult) {
            return [
                'success' => false,
                'message' => 'Ошибка применения изменения к контакту'
            ];
        }
    }

    if ($status === 'rejected') {
    }
    
    // Обновляем статус
    $changes[$index]['status'] = $status;
    $changes[$index]['updated_at'] = date('Y-m-d H:i:s');
    
    // Сохраняем обратно в файл
    $logFile = __DIR__ . '/modules/timofey.connector1c/logs/changes_tracker.json';
    $result = file_put_contents(
        $logFile, 
        json_encode($changes, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE)
    );
    
    if ($result === false) {
        return [
            'success' => false,
            'message' => 'Ошибка сохранения статуса изменения'
        ];
    }
    
    return [
        'success' => true,
        'message' => 'Статус успешно обновлен'
    ];
}

function updateContact($change) {
    try {
        $contact = new \CCrmContact(false);
        $updateFields = [
            $change['field'] => $change['new_value']
        ];
        
        $result = $contact->Update($change['contact_id'], $updateFields, true, true);
        
        if ($result) {
            return true;
        }
        
        sendBitrixNotification("Ошибка применения изменения контакта #" . $change['contact_id']);
        return false;
        
    } catch (Exception $e) {
        error_log("Ошибка применения изменения контакта: " . $e->getMessage());
        sendBitrixNotification("Ошибка применения изменения: " . $e->getMessage());
        return false;
    }
}

function sendBitrixNotification($message) {
    if (CModule::IncludeModule("im")) {
        $arMessageFields = array(
            "TO_USER_ID" => 3,
            "FROM_USER_ID" => 3,
            "NOTIFY_TYPE" => 2,
            "NOTIFY_TAG" => "",
            "NOTIFY_MESSAGE" => $message,
        );

        CIMNotify::Add($arMessageFields);
    }
}

function deals($timestamp) {
    $currentDate = new DateTime();
    //30.11.2025
    if($timestamp > 1764508244){
        httpRequest('https://b24.trimiata.ru/bitrix/admin/settings.php?lang=ru&mid=timofey.connector1c&mid_menu=1&action=update&date=' . $timestamp);
    }
	return 'deals(' . $currentDate->getTimestamp() . ');';
}


function dailySync() {
    httpRequest('https://b24.trimiata.ru/bitrix/admin/settings.php?lang=ru&mid=timofey.connector1c&mid_menu=1&action=count');
	return 'dailySync();';
}

function httpRequest($url) {
    $httpClient = new HttpClient();
    $httpClient->setHeader('Content-Type', 'application/json', true);
    $httpClient->setAuthorization('support', '789Vf6yjgftedQ!!');
    $response = $httpClient->get($url);
}

?>