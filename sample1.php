<?php

namespace app\common\parserJobs;

use app\common\MaxItemsInterrputer;
use app\common\parsers\ParserInterface;
use app\models\general\Category;
use app\models\general\Item;
use app\models\general\ParserJobRun;
use app\models\general\Region;
use app\services\FlashNotificationService\FlashNotificationServiceInterface;
use app\services\Logger;
use app\services\NotifierService\Attachment;
use app\services\NotifierService\NotifierService;
use app\services\PaymentService;
use yii\base\BaseObject;
use yii\queue\JobInterface;

class ParserJob extends BaseObject implements JobInterface
{

    public int $parser_job_run_id;
    private FlashNotificationServiceInterface $notificationService;
    private NotifierService $notifierService;
    public function __construct($config = [])
    {
        parent::__construct($config);
        $this->notificationService = \Yii::$container->get(FlashNotificationServiceInterface::class);
        $this->notifierService = new NotifierService();
    }

    public function execute($queue)
    {

        $paymentService = new PaymentService();
        $jobRun = ParserJobRun::findOne($this->parser_job_run_id);

        $counter = new \stdClass();
        $counter->countItems = 0;

        $maxItems = $jobRun->settings['max_items'] ?? 0;

        $logger = new Logger('ParserJobRun', $jobRun->id);

        /**
         * @var class-string<ParserInterface> $parserClass
         */
        $parserClass = $jobRun->parserJob->parser->class_name;

        $parser = new $parserClass($jobRun->settings ?? [], ['interrupter' => MaxItemsInterrputer::class, 'maxItems' => $maxItems, 'logger' => $logger,

            'checkStopFlag' => function () use ($counter, $maxItems) {
                return $counter->countItems >= $maxItems;
            },

            "notifyCallback" => function(array $data) use ($jobRun) {
                $subject = $data['subject'] ?? \Yii::t('app', "Notification from importer job '{jobName}' at {date}",
                    [
                        'jobName' => $jobRun->parserJob->name,
                        'date' => (new \DateTime())->format('Y-m-d H:i:s')
                    ]);
                $message = $data['message'] ?? \Yii::t('app', "Job results are attached");
                $filepath = $data['filename'] ?? null;

                $attachments = [];

                if ($filepath !== null) {
                    $filename = basename($filepath);
                    $content = file_get_contents($filepath);

                    $attachments = [
                        new Attachment($filename, $content)
                    ];
                }

                $this->notifierService->notifyUser($subject, $message, $attachments);
            }

        ]);

        if (!($jobRun->settings['parse_category'] ?? false) && !($jobRun->settings['parse_regions'] ?? false)) {

            $amount = $parserClass::getTariffRunPriceCoeff() * \Yii::$app->params['baseRunTariffPrice'] + $parser->getMaxItems() * $parserClass::getTariffItemPriceCoeff() * \Yii::$app->params['baseItemTariffPrice'];

            try {
                $balance = $paymentService->getBalance();
            } catch (\Throwable $throwable) {
                $logger->debug($throwable->getMessage() . $throwable->getTraceAsString());
                return;
            }

            $logger->log(Logger::LEVEL_INFO, "Current balance: " . $balance);

            if ($balance < $amount) {
                $logger->log(Logger::LEVEL_INFO, "Not enough balance: " . $balance . ", need $amount");
                $jobRun->status = ParserJobRun::STATUS_ERROR;
                $jobRun->save();
                return;
            }

            try {
                $payment = $paymentService->createHoldPaymentForJobRun($jobRun, $amount);
            } catch (\Throwable $throwable) {
                $logger->debug($throwable->getMessage() . $throwable->getTraceAsString());
                return;
            }

            if (!$payment->id) {
                $logger->debug(print_r($payment->errors, 1));
            }

        }

        $jobRun->date_started = (new \DateTime())->format('c');
        $jobRun->status = ParserJobRun::STATUS_PROCESSING;
        if (!$jobRun->save()) {
            $logger->debug(print_r($jobRun->errors, 1));
        }

        if ($jobRun->settings['parse_category'] ?? false) {
            try {
                $parser->parseCategories(function (array $item) use ($counter, $logger, $parser) {
                    $category = Category::find()->where(['external_id'=>$item['external_id'], 'source'=>$parser->getInstanceName()])->one();

                    /**
                     * @var Category $parent
                     */
                    $parent = Category::find()->where(['external_id'=>$item['parent'], 'source'=>$parser->getInstanceName()])->one();
                    $parent_id = $parent?->id;

                    if (!$category) {
                        $category = new Category();
                    }

                    $category->name = $item['name'];
                    $category->parent_id = $parent_id;
                    $category->external_id = (string)$item['external_id'];
                    $category->source = (string)$parser->getInstanceName();
                    if (!$category->save()) {
                        $logger->log(Logger::LEVEL_INFO, "Can not save category: " . print_r($category->errors, 1));
                    }
                });
            } catch (\Throwable $throwable) {
                $logger->log(Logger::LEVEL_INFO, "Error while parsing categories: " . $throwable->getMessage() . $throwable->getTraceAsString());
            }
        } elseif ($jobRun->settings['parse_regions'] ?? false) {
            try {
                $parser->parseRegions(function (array $item) use ($counter, $logger, $parser) {

                    $region = Region::find()->where(['external_id'=>$item['external_id'], 'source'=>$parser->getInstanceName()])->one();

                    /**
                     * @var Region $parent
                     */
                    $parent = Region::find()->where(['external_id'=>$item['parent'], 'source'=>$parser->getInstanceName()])->one();
                    $parent_id = $parent?->id;

                    if (!$region) {
                        $region = new Region();
                    }

                    $region->name = $item['name'];
                    $region->parent_id = $parent_id;
                    $region->external_id = (string)$item['external_id'];
                    $region->source = (string)$parser->getInstanceName();
                    if (!$region->save()) {
                        $logger->log(Logger::LEVEL_INFO, "Can not save region: " . print_r($region->errors, 1));
                    }
                });
            } catch (\Throwable $throwable) {
                $logger->log(Logger::LEVEL_INFO, "Error while parsing regions: " . $throwable->getMessage() . $throwable->getTraceAsString());
            }
        } else {
            try {
                $parser->parse(function (array $item) use ($maxItems, $counter, $logger, $parser, $jobRun) {
                    if ($counter->countItems < $maxItems) {

                        /**
                         * @var null|Category $category
                         */
                        $category = isset($item['category']) ? Category::find()->where(['external_id' => $item['category']])->one() : null;

                        /**
                         * @var null|Item $sameCorellationItem
                         */
                        $sameCorellationItem = Item::find()->where(['main_external_id' => $item['external_id'], 'parser_job_run_id' => $this->parser_job_run_id])->one();
                        if ($sameCorellationItem) {
                            $corellationUuid = $sameCorellationItem->correlation_uuid;
                        } else {
                            $corellationUuid = \Ramsey\Uuid\Uuid::uuid4()->toString();
                        }


                        $it = new Item();
                        $it->name = preg_replace('/\s+/', ' ', trim($item['name'] ?? ''));
                        $it->price = $item['price'] ?? '0';
                        $it->brand_name = $item['brand_name'] ?? null;
                        $it->external_id = (string)$item['external_id'] . (isset($item['variant']) ? (' | ' . $item['variant']) : '');
                        $it->variant = $item['variant'] ?? null;
                        $it->main_external_id = (string)$item['external_id'];
                        $it->url = $item['url'] ?? $item['external_id'];
                        $it->correlation_uuid = $corellationUuid;
                        $it->source = $parser->getInstanceName();
                        $it->parser_job_run_id = $this->parser_job_run_id;
                        $it->images = $item['images'];
                        $it->description = $item['description'] ?? null;
                        $it->category_id = $category?->id;
                        $it->params = $item['params'] ?? [];
                        if (!$it->save()) {
                            $logger->log(Logger::LEVEL_INFO, "Can not save item: " . print_r($it->errors, 1));
                            return false;
                        } else {
                            $counter->countItems++;
                            return true;
                        }
                    } else {
                        // skip...
                        return false;
                    }
                });
            } catch (\Throwable $throwable) {
                $logger->log(Logger::LEVEL_INFO, "Error while parsing items: " . $throwable->getMessage() . $throwable->getTraceAsString());
            }
        }

        if (!($jobRun->settings['parse_category'] ?? false)) {
            $numItems = Item::find()->where(['parser_job_run_id'=>$jobRun->id])->count();
            $amount = $parserClass::getTariffRunPriceCoeff() * \Yii::$app->params['baseRunTariffPrice'] + $numItems * $parserClass::getTariffItemPriceCoeff() * \Yii::$app->params['baseItemTariffPrice'];
            // TODO: correct amount to be equals to price_per_item * countItems
            $paymentService->unholdPayment($payment, $amount);
        }

        $jobRun->date_finished = (new \DateTime())->format('c');
        $jobRun->status = ParserJobRun::STATUS_FINISHED;
        if (!$jobRun->save()) {
            $logger->log(Logger::LEVEL_INFO, "Can not save jobRun: " . print_r($jobRun->errors, 1));
        }

        $this->notificationService->notify("JobRun " . $jobRun->id . " for parser job '" . $jobRun->parserJob->name . "' finished");

    }
}
