<?php

namespace app\services\kafka\handlers;

use app\models\Bot;
use app\services\BotService;
use app\services\kafka\events\BwBotContainerStoppedEvent;
use app\services\kafka\events\LkStopBotContainerEvent;
use app\services\KafkaService;

class LkStopBotContainerEventHandler
{
    public function handle(LkStopBotContainerEvent $event): void
    {
        $bot = Bot::findOne($event->getBotId());

        $bs = new BotService();
        if ($bs->canStopContainer($bot)) {
            $result = $bs->stopContainer($bot);            
            $status = "stopped";
        } else {
            $status = "failed";
        }

        $ks = new KafkaService();
        $event = new BwBotContainerStoppedEvent($event->getBotId(), $status);
        $ks->produce("event_bus", $event->getPayload());
    }
}
