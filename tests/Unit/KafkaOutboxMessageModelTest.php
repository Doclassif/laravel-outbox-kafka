<?php

namespace Tests\Unit\Modules\Kafka;

use Modules\Kafka\Enums\KafkaOutboxStatus;
use Modules\Kafka\Models\KafkaOutboxMessage;
use Tests\TestCase;

class KafkaOutboxMessageModelTest extends TestCase
{
    public function test_kafka_outbox_message_factory_creates_pending_message_with_casts(): void
    {
        $message = KafkaOutboxMessage::factory()->create();

        $this->assertDatabaseHas('kafka_outbox_messages', [
            'id' => $message->id,
            'topic' => $message->topic,
            'status' => KafkaOutboxStatus::PENDING->value,
            'attempts' => 0,
        ]);
        $this->assertSame(KafkaOutboxStatus::PENDING, $message->status);
        $this->assertIsArray($message->payload);
        $this->assertNotNull($message->available_at);
        $this->assertNull($message->sent_at);
    }

    public function test_kafka_outbox_message_factory_has_sent_state(): void
    {
        $message = KafkaOutboxMessage::factory()->sent()->create();

        $this->assertSame(KafkaOutboxStatus::SENT, $message->status);
        $this->assertNotNull($message->sent_at);
    }
}
