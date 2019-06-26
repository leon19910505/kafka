<?php

/**
 * @param $topic string
 * @param $msg array
 * @throws Exception
 */
function kafka_dispatch($topic, $msg)
{
    $producer = Zhizhong\Kafka\Producer::getInstance();

    if (count($msg) == count($msg, 1)) {

        $producer->produce($topic, $msg);
    } else {
        foreach ($msg as $item) {
            $producer->produce($topic, $item);
        }
    }

}
