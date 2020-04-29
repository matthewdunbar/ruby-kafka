# frozen_string_literal: true

describe KafkaLegacy::Protocol::Message do
  include KafkaLegacy::Protocol

  it "decodes message sets" do
    message1 = KafkaLegacy::Protocol::Message.new(value: "hello")
    message2 = KafkaLegacy::Protocol::Message.new(value: "good-day")

    message_set = KafkaLegacy::Protocol::MessageSet.new(messages: [message1, message2])

    data = StringIO.new
    encoder = KafkaLegacy::Protocol::Encoder.new(data)

    message_set.encode(encoder)

    data.rewind

    decoder = KafkaLegacy::Protocol::Decoder.new(data)
    new_message_set = KafkaLegacy::Protocol::MessageSet.decode(decoder)

    expect(new_message_set.messages).to eq [message1, message2]
  end

  it "skips the last message if it has been truncated" do
    message1 = KafkaLegacy::Protocol::Message.new(value: "hello")
    message2 = KafkaLegacy::Protocol::Message.new(value: "good-day")

    message_set = KafkaLegacy::Protocol::MessageSet.new(messages: [message1, message2])

    data = StringIO.new
    encoder = KafkaLegacy::Protocol::Encoder.new(data)

    message_set.encode(encoder)

    data.rewind
    data.truncate(data.size - 1)

    decoder = KafkaLegacy::Protocol::Decoder.new(data)
    new_message_set = KafkaLegacy::Protocol::MessageSet.decode(decoder)

    expect(new_message_set.messages).to eq [message1]
  end

  it "raises MessageTooLargeToRead if the first message in the set has been truncated" do
    message = KafkaLegacy::Protocol::Message.new(value: "hello")

    message_set = KafkaLegacy::Protocol::MessageSet.new(messages: [message])

    data = StringIO.new
    encoder = KafkaLegacy::Protocol::Encoder.new(data)

    message_set.encode(encoder)

    data.rewind
    data.truncate(data.size - 1)

    decoder = KafkaLegacy::Protocol::Decoder.new(data)

    expect {
      KafkaLegacy::Protocol::MessageSet.decode(decoder)
    }.to raise_exception(KafkaLegacy::MessageTooLargeToRead)
  end

  describe '.decode' do
    let(:instrumenter) { KafkaLegacy::Instrumenter.new(client_id: "test") }
    let(:compressor) { KafkaLegacy::Compressor.new(codec_name: :snappy, threshold: 1, instrumenter: instrumenter) }

    def encode(messages: [], wrapper_message_offset: -1)
      message_set = KafkaLegacy::Protocol::MessageSet.new(messages: messages)
      compressed_message_set = compressor.compress(message_set, offset: wrapper_message_offset)
      KafkaLegacy::Protocol::Encoder.encode_with(compressed_message_set)
    end

    def decode(data)
      decoder = KafkaLegacy::Protocol::Decoder.from_string(data)
      KafkaLegacy::Protocol::MessageSet
        .decode(decoder)
        .messages
    end

    it "sets offsets correctly for compressed messages with relative offsets" do
      message1 = KafkaLegacy::Protocol::Message.new(value: "hello1", offset: 0)
      message2 = KafkaLegacy::Protocol::Message.new(value: "hello2", offset: 1)
      message3 = KafkaLegacy::Protocol::Message.new(value: "hello3", offset: 2)

      data = encode(messages: [message1, message2, message3], wrapper_message_offset: 1000)
      messages = decode(data)

      expect(messages.map(&:offset)).to eq [998, 999, 1000]
    end

    it "sets offsets correctly for compressed messages with relative offsets on a compacted topic" do
      message1 = KafkaLegacy::Protocol::Message.new(value: "hello1", offset: 0)
      message2 = KafkaLegacy::Protocol::Message.new(value: "hello2", offset: 2)
      message3 = KafkaLegacy::Protocol::Message.new(value: "hello3", offset: 3)

      data = encode(messages: [message1, message2, message3], wrapper_message_offset: 1000)
      messages = decode(data)

      expect(messages.map(&:offset)).to eq [997, 999, 1000]
    end

    it "keeps the predefined offsets for messages delivered in 0.9 format" do
      message1 = KafkaLegacy::Protocol::Message.new(value: "hello1", offset: 997)
      message2 = KafkaLegacy::Protocol::Message.new(value: "hello2", offset: 999)
      message3 = KafkaLegacy::Protocol::Message.new(value: "hello3", offset: 1000)

      data = encode(messages: [message1, message2, message3], wrapper_message_offset: 1000)
      messages = decode(data)

      expect(messages.map(&:offset)).to eq [997, 999, 1000]
    end
  end
end
