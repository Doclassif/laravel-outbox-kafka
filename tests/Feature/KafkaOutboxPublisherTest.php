<?php

namespace Tests\Feature\Modules\Kafka;

use Junges\Kafka\Contracts\MessageProducer;
use Junges\Kafka\Facades\Kafka;
use Mockery;
use Modules\Kafka\Actions\KafkaOutboxPublisher;
use Modules\Kafka\Enums\KafkaOutboxStatus;
use Modules\Kafka\Jobs\PublishKafkaOutboxMessage;
use Modules\Kafka\Models\KafkaOutboxMessage;
use RuntimeException;
use Tests\TestCase;

class KafkaOutboxPublisherTest extends TestCase
{
    public function test_publish_stores_pending_outbox_message(): void
    {
        app(KafkaOutboxPublisher::class)->publish('test.topic', ['id' => 123]);

        $this->assertDatabaseHas('kafka_outbox_messages', [
            'topic' => 'testing.test.topic',
            'status' => KafkaOutboxStatus::PENDING,
            'attempts' => 0,
        ]);
    }

    public function test_publish_job_marks_outbox_message_sent(): void
    {
        $message = KafkaOutboxMessage::create([
            'topic' => 'testing.test.topic',
            'payload' => ['id' => 123],
            'status' => KafkaOutboxStatus::PENDING,
            'available_at' => now()->subSecond(),
        ]);

        $producer = Mockery::mock(MessageProducer::class);
        $producer->shouldReceive('onTopic')
            ->once()
            ->with('testing.test.topic')
            ->andReturnSelf();
        $producer->shouldReceive('withBody')
            ->once()
            ->with(['id' => 123])
            ->andReturnSelf();
        $producer->shouldReceive('send')
            ->once()
            ->andReturnTrue();

        Kafka::shouldReceive('publish')
            ->once()
            ->andReturn($producer);

        (new PublishKafkaOutboxMessage($message->id))->handle();

        $this->assertDatabaseHas('kafka_outbox_messages', [
            'topic' => 'testing.test.topic',
            'status' => KafkaOutboxStatus::SENT,
            'attempts' => 0,
        ]);
    }

    public function test_publish_keeps_message_pending_when_kafka_is_unavailable(): void
    {
        Kafka::shouldReceive('publish')
            ->once()
            ->andThrow(new RuntimeException('Kafka unavailable'));

        $message = KafkaOutboxMessage::create([
            'topic' => 'testing.test.topic',
            'payload' => ['id' => 123],
            'status' => KafkaOutboxStatus::PENDING,
            'available_at' => now()->subSecond(),
        ]);

        (new PublishKafkaOutboxMessage($message->id))->handle();

        $this->assertDatabaseHas('kafka_outbox_messages', [
            'topic' => 'testing.test.topic',
            'status' => KafkaOutboxStatus::PENDING,
            'attempts' => 1,
            'last_error' => 'Kafka unavailable',
        ]);
    }

    public function test_flush_command_publishes_pending_messages(): void
    {
        $producer = Mockery::mock(MessageProducer::class);
        $producer->shouldReceive('onTopic')
            ->once()
            ->with('testing.test.topic')
            ->andReturnSelf();
        $producer->shouldReceive('withBody')
            ->once()
            ->with(['id' => 123])
            ->andReturnSelf();
        $producer->shouldReceive('send')
            ->once()
            ->andReturnTrue();

        Kafka::shouldReceive('publish')
            ->once()
            ->andReturn($producer);

        KafkaOutboxMessage::create([
            'topic' => 'testing.test.topic',
            'payload' => ['id' => 123],
            'status' => KafkaOutboxStatus::PENDING,
            'available_at' => now()->subSecond(),
        ]);

        $this->artisan('kafka-outbox:flush')
            ->expectsOutput('Dispatched 1 Kafka outbox job(s).')
            ->assertSuccessful();

        $this->assertDatabaseHas('kafka_outbox_messages', [
            'topic' => 'testing.test.topic',
            'status' => KafkaOutboxStatus::SENT,
        ]);
    }
}
