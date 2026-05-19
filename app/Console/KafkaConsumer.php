<?php

namespace Modules\Kafka\Console;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use Junges\Kafka\Facades\Kafka;

abstract class KafkaConsumer extends Command
{
    /** @return class-string */
    abstract protected function handler(): callable|string;

    /** @return string[] */
    abstract protected function topics(): array;

    protected function autoCommit(): bool
    {
        return true;
    }
    /** @return string[]|null */
    protected function brokers(): ?array
    {
        return null;
    }

    /** @return string|null */
    protected function groupId(): string
    {
        return config('kafka.consumer_group_id');
    }

    public function handle(): int
    {
        $handlerClass = $this->handler();

        $topics = array_map(function ($item)
        {
            return config('kafka.topic_prefix', app()->environment()).'.'.$item;
        }, $this->topics());

        Log::info('Starting consumer', [
            'handler' => $handlerClass,
            'topics' => $topics,
        ]);

        Kafka::consumer()
            ->subscribe($topics)
            ->withHandler(app($handlerClass))
            ->withConsumerGroupId($this->groupId())
            ->withBrokers($this->brokers())
            ->withAutoCommit($this->autoCommit())
            ->withSasl(
                config('kafka.sasl.username'),
                config('kafka.sasl.password'),
                config('kafka.sasl.mechanisms', 'PLAINTEXT'),
                config('kafka.securityProtocol', 'PLAINTEXT')
            )
            ->build()
            ->consume();

        return self::SUCCESS;
    }
}