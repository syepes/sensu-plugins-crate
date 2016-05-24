#!/usr/bin/env ruby
#
# extension-crate-events
#
# DESCRIPTION:
#   Crate.IO Sensu extension that stores Sensu Events in Crate using the REST-API
#   Events will be buffered until they reach the configured length or maximum age (buffer_size & buffer_max_age)
#
# OUTPUT:
#   event data
#
# PLATFORMS:
#   Linux
#
# DEPENDENCIES:
#   gem: sensu-plugin
#
# USAGE:
#   0) Crate.IO destination table: curl -vXPOST 127.0.0.1:4200/_sql?pretty -d '{"stmt":"CREATE TABLE IF NOT EXISTS events (id string primary key, timestamp timestamp, action string, status string, occurrences integer, client object, check object)"}'
#   1) Add the extension-crate-events.rb to the Sensu extensions folder (/etc/sensu/extensions)
#   2) Create the Sensu configuration for the extention inside the sensu config folder (/etc/sensu/conf.d)
#      echo '{ "crate-events": { "hostname": "127.0.0.1", "port": "4200", "table": "events" } }' >/etc/sensu/conf.d/crate_cfg.json
#      echo '{ "handlers": { "default": { "type": "set", "handlers": ["crate-events"] } } }' >/etc/sensu/conf.d/crate_handler.json
#
#
# NOTES:
#
# LICENSE:
#   Copyright 2016 Sebastian YEPES <syepes@gmail.com>
#   Released under the same terms as Sensu (the MIT license); see LICENSE for details.
#

require 'net/http'
require 'json'

module Sensu::Extension
  class Crate < Handler

    @@extension_name = 'crate-events'

    def name
      @@extension_name
    end

    def description
      'Historization of Sensu Event in Crate.IO'
    end

    def post_init
      crate_config = settings[@@extension_name]
      validate_config(crate_config)

      hostname         = crate_config['hostname']
      port             = crate_config['port'] || 4200
      @table           = crate_config['table']
      ssl              = crate_config['ssl'] || false
      ssl_cert         = crate_config['ssl_cert']
      protocol         = if ssl then 'https' else 'http' end
      @BUFFER_SIZE     = crate_config['buffer_size'] || 3
      @BUFFER_MAX_AGE  = crate_config['buffer_max_age'] || 300

      @uri = URI("#{protocol}://#{hostname}:#{port}/_sql")
      @http = Net::HTTP::new(@uri.host, @uri.port)

      # To be implemented in Crate.IO
      if ssl
        @http.ssl_version = :TLSv1
        @http.use_ssl = true
        @http.verify_mode = OpenSSL::SSL::VERIFY_PEER
        @http.ca_file = ssl_cert
      end

      @buffer = []
      @buffer_flushed = Time.now.to_i

      @logger.info("#{@@extension_name}: Successfully initialized config: hostname: #{hostname}, port: #{port}, table: #{@table}, uri: #{@uri.to_s}, buffer_size: #{@BUFFER_SIZE}, buffer_max_age: #{@BUFFER_MAX_AGE}")
    end

    def run(event)
      begin
        if buffer_too_old? or buffer_too_big?
          flush_buffer
        end

        event = JSON.parse(event)

        # Convert timestamps to ms as they are directly supported by Crate (https://crate.io/docs/reference/sql/data_types.html#timestamp)
        event['timestamp'] *= 1000
        event['client']['timestamp'] *= 1000 if event['client'].key?('timestamp')
        event['check']['issued'] *= 1000 if event['check'].key?('issued')
        event['check']['executed'] *= 1000 if event['check'].key?('executed')

        evt = {
          :id => event['id'],
          :timestamp => event['timestamp'],
          :action => event['action'],
          :status => event_status(event['check']['status']),
          :occurrences => event['occurrences'],
          :client => event['client'],
          :check => event['check']
        }
        @logger.debug("#{@@extension_name}: Event: #{evt['action']} -> #{evt['status']} - #{evt['check']['output']})")

        @buffer.push(evt)
        @logger.info("#{@@extension_name}: Stored Event in buffer (#{@buffer.length}/#{@BUFFER_SIZE})")
      rescue => e
        @logger.error("#{@@extension_name}: Unable to buffer Event: #{event} - #{e.message} - #{e.backtrace.to_s}")
      end

      yield("#{@@extension_name}: handler finished", 0)
    end

    private
    def send_to_crate(events)
      # TODO Refactor Ugly JSON workaround
      bulk = events.collect { |e|
        [e[:id], e[:timestamp], e[:action], e[:status], e[:occurrences], e[:client].to_json.to_s.gsub(/"(\w+)"\s*:/, "\\1:").gsub(/[\\]/,""), e[:check].to_json.to_s.gsub(/"(\w+)"\s*:/, "\\1:").gsub(/[\\]/,"")]
      }

      data = {
        :stmt => "INSERT INTO #{@table} (id, timestamp, action, status, occurrences, client, check) VALUES (?, ?, ?, ?, ?, ?, ?)",
        :bulk_args => bulk
      }

      request = Net::HTTP::Post.new(@uri.request_uri, 'Content-Type' => 'application/json')
      # TODO Refactor Ugly JSON workaround
      request.body = data.to_json.to_s.gsub(/"(\w+)"\s*:/, "\\1:").gsub(/[\\]/,"").gsub(/"\{/, "{").gsub(/\}"/, "}")

      @logger.debug("#{@@extension_name}: Writing Event: #{request.body} to Crate: #{@uri.to_s}")

      response = @http.request(request)
      if response.code.to_i != 200
        @logger.error("#{@@extension_name}: Writing Event to Crate: response code = #{response.code}, body = #{response.body}")
      else
        @logger.info("#{@@extension_name}: Sent #{@buffer.length} Events to Crate")
        @logger.debug("#{@@extension_name}: Writing Event to Crate: response code = #{response.code}, body = #{response.body}")
      end
    end

    def flush_buffer
      send_to_crate(@buffer)
      @buffer = []
      @buffer_flushed = Time.now.to_i
    end

    def buffer_too_old?
      buffer_age = Time.now.to_i - @buffer_flushed
      buffer_age >= @BUFFER_MAX_AGE
    end

    def buffer_too_big?
      @buffer.length >= @BUFFER_SIZE
    end

    def event_status(status)
      case status
      when 0
        'OK'
      when 1
        'WARNING'
      when 2
        'CRITICAL'
      else
        'UNKNOWN'
      end
    end

    def validate_config(config)
      if config.nil?
        raise ArgumentError, "No configuration for #{@@extension_name} provided. exiting..."
      end

      ["hostname", "port", "table"].each do |required_setting|
        if config[required_setting].nil?
          raise ArgumentError, "Required setting #{required_setting} not provided to extension. this should be provided as json element with key '#{@@extension_name}'. exiting..."
        end
      end
    end

  end
end