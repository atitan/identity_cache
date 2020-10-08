# frozen_string_literal: true
require 'dalli/cas/client'

module IdentityCache
  module MemCacheStoreCAS
    def cas(name, options = nil)
      options = merged_options(options)
      key = normalize_key(name, options)

      rescue_error_with(false) do
        instrument(:cas, key, options) do
          @data.with do |connection|
            connection.cas(key, options[:expires_in].to_i, options) do |raw_value|
              entry = deserialize_entry(raw_value)
              value = yield entry.value
              entry = ActiveSupport::Cache::Entry.new(value, **options)
              options[:raw] ? entry.value.to_s : entry
            end
          end
        end
      end
    end

    def cas_multi(*names, **options)
      return if names.empty?

      options = merged_options(options)
      keys_to_names = names.each_with_object({}) { |name, hash| hash[normalize_key(name, options)] = name }
      keys = keys_to_names.keys
      rescue_error_with(false) do
        instrument(:cas_multi, keys, options) do
          raw_values = @data.get_multi_cas(keys)

          values = {}
          raw_values.each do |key, raw_value|
            entry = deserialize_entry(raw_value.first)
            values[keys_to_names[key]] = entry.value unless entry.expired?
          end

          updates = yield values

          updates.each do |name, value|
            key = normalize_key(name, options)
            cas_id = raw_values[key].last
            entry = ActiveSupport::Cache::Entry.new(value, **options)
            payload = options[:raw] ? entry.value.to_s : entry
            @data.replace_cas(key, payload, options[:expires_in].to_i, cas_id, options)
          end
        end
      end
    end

    # Memcache keys are binaries. So we need to force their encoding to binary
    # before applying the regular expression to ensure we are escaping all
    # characters properly.
    def normalize_key(key, options)
      key = super.dup
      key = key.force_encoding(Encoding::ASCII_8BIT)
      key = key.gsub(ESCAPE_KEY_CHARS) { |match| "%#{match.getbyte(0).to_s(16).upcase}" }
      key = "#{key[0, 213]}:md5:#{ActiveSupport::Digest.hexdigest(key)}" if key.size > 250
      key
    end
  end
end
