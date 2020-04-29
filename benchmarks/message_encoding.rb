# frozen_string_literal: true

require "kafka-legacy"

ready "message serialization" do
  before do
    message = KafkaLegacy::Protocol::Message.new(
      value: "hello",
      key: "world",
    )

    @io = StringIO.new
    encoder = KafkaLegacy::Protocol::Encoder.new(@io)
    message.encode(encoder)

    @decoder = KafkaLegacy::Protocol::Decoder.new(@io)
  end

  go "decoding" do
    @io.rewind
    KafkaLegacy::Protocol::Message.decode(@decoder)
  end
end
