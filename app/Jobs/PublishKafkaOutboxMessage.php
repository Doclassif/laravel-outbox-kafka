<?php

namespace Modules\Kafka\Jobs;

use Illuminate\Contracts\Queue\ShouldBeUnique;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable as QueueableJob;
use Junges\Kafka\Facades\Kafka;
use Modules\Kafka\Enums\KafkaOutboxStatus;
use Modules\Kafka\Models\KafkaOutboxMessage;
use Throwable;

class PublishKafkaOutboxMessage implements ShouldBeUnique, ShouldQueue
{
    use QueueableJob;

    public int $tries = 1;

    public int $uniqueFor = 300;

    public function __construct(
        private readonly int $messageId,
    ) {}

    public function uniqueId(): string
    {
        return (string) self::class .'-'.$this->messageId;
    }

    public function handle(): void
    {
        $message = KafkaOutboxMessage::find($this->messageId);

        if (! $message || $message->status === KafkaOutboxStatus::SENT)
        {
            return;
        }

        try
        {
            $producer = Kafka::publish()
                ->withSasl(
                    config('kafka.sasl.username'),
                    config('kafka.sasl.password'),
                    config('kafka.sasl.mechanisms', 'PLAINTEXT'),
                    config('kafka.securityProtocol', 'PLAINTEXT')
                )
                ->onTopic($message->topic)
                ->withBody($message->payload);

            if ($message->key !== null)
            {
                $producer->withKafkaKey($message->key);
            }

            $producer->send();

            $message->forceFill([
                'status' => KafkaOutboxStatus::SENT,
                'last_error' => null,
                'sent_at' => now(),
            ])->save();
        }
        catch (Throwable $exception)
        {
            $message->forceFill([
                'attempts' => $message->attempts + 1,
                'last_error' => mb_substr($exception->getMessage(), 0, 65535),
                'available_at' => now()->addMinute(),
            ])->save();
        }
    }
}
