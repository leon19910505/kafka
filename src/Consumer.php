<?php
/**
 * Created by PhpStorm.
 * User: Zhizhong
 * Date: 2019/6/17
 * Time: 上午10:24
 */

namespace Zhizhong\Kafka;

use Exception;

class Consumer
{
    private $consumer;

    public function __construct($topic)
    {
        $conf = new \RdKafka\Conf();
        $brokers = env("KAFKA_BROKERS");
        if (!$brokers) {
            throw new Exception('please configure KAFKA_BROKERS param in env first');
        }

        // Configure the group.id. All consumer with the same group.id will consume different partitions.
        $conf->set('group.id', $topic);
        $conf->set('metadata.broker.list', $brokers);
        $topicConf = new \RdKafka\TopicConf();

        // Set where to start consuming messages when there is no initial offset in
        // offset store or the desired offset is out of range.
        // 'smallest': start from the beginning
        $topicConf->set('auto.offset.reset', 'smallest');
        $topicConf->set('enable.auto.commit', true);
        $topicConf->set('auto.commit.interval.ms', 5000);

        // Set the configuration to use for subscribed/assigned topics
        $conf->setDefaultTopicConf($topicConf);

        $this->consumer = new \RdKafka\KafkaConsumer($conf);

        // Subscribe to topic 'test'
        $this->consumer->subscribe([$topic]);
    }


    public function consume()
    {
        $message = $this->consumer->consume(120 * 1000);
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                echo "No more messages; will wait for more\n";
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                echo "Timed out\n";
                break;
            default:
                throw new Exception($message->errstr(), $message->err);
                break;
        }
        $json = json_decode($message->payload, true);
        if (is_array($json)) {
            return $json;
        }
    }
}