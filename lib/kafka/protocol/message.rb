require "stringio"
require "zlib"

module Kafka
  module Protocol

    # ## API Specification
    #
    #     Message => Crc MagicByte Attributes Key Value
    #         Crc => int32
    #         MagicByte => int8
    #         Attributes => int8
    #         (optional) Timestamp => int64
    #         Key => bytes
    #         Value => bytes
    #
    class Message
      attr_reader :key, :value, :codec_id, :offset, :timestamp

      attr_reader :bytesize, :create_time

      def initialize(value:, key: nil, create_time: Time.now, codec_id: 0, offset: -1, timestamp: nil)
        @key = key
        @value = value
        @codec_id = codec_id
        @offset = offset
        @create_time = create_time
        @timestamp = timestamp

        @bytesize = @key.to_s.bytesize + @value.to_s.bytesize
      end

      def encode(encoder)
        data = encode_with_crc

        encoder.write_int64(offset)
        encoder.write_bytes(data)
      end

      def ==(other)
        @key == other.key &&
          @value == other.value &&
          @codec_id == other.codec_id &&
          @offset == other.offset &&
          @timestamp == other.timestamp
      end

      def compressed?
        @codec_id != 0
      end

      # @return [Kafka::Protocol::MessageSet]
      def decompress
        codec = Compression.find_codec_by_id(@codec_id)

        # For some weird reason we need to cut out the first 20 bytes.
        data = codec.decompress(value)
        message_set_decoder = Decoder.from_string(data)

        MessageSet.decode(message_set_decoder)
      end

      def self.decode(decoder)
        offset = decoder.int64
        message_decoder = Decoder.from_string(decoder.bytes)

        crc = message_decoder.int32
        magic_byte = message_decoder.int8
        unless magic_byte == 0 || magic_byte == 1
          raise Kafka::Error, "Unsupported message with magic byte: #{magic_byte}"
        end
        attributes = message_decoder.int8

        timestamp = message_decoder.int64 if magic_byte > 0
        key = message_decoder.bytes
        value = message_decoder.bytes

        # The codec id is encoded in the three least significant bits of the
        # attributes.
        codec_id = attributes & 0b111

        new(key: key, value: value, codec_id: codec_id, offset: offset, timestamp: timestamp)
      end

      private

      def encode_with_crc
        buffer = StringIO.new
        encoder = Encoder.new(buffer)

        data = encode_without_crc
        crc = Zlib.crc32(data)

        encoder.write_int32(crc)
        encoder.write(data)

        buffer.string
      end

      def encode_without_crc
        buffer = StringIO.new
        encoder = Encoder.new(buffer)

        encoder.write_int8(@timestamp.nil? ? 0 : 1) # magic byte
        encoder.write_int8(@codec_id)
        encoder.write_int64(@timestamp) unless @timestamp.nil?
        encoder.write_bytes(@key)
        encoder.write_bytes(@value)

        buffer.string
      end
    end
  end
end
