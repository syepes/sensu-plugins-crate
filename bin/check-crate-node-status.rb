#! /usr/bin/env ruby
#
#  check-crate-node-status
#
# DESCRIPTION:
#   This plugin checks the Crate.IO node status
#
# OUTPUT:
#   plain text
#
# PLATFORMS:
#   Linux
#
# DEPENDENCIES:
#   gem: sensu-plugin
#   gem: rest-client
#
# USAGE:
#   check-crate-node-status --help
#
# NOTES:
#
# LICENSE:
#   Copyright 2016 Sebastian YEPES <syepes@gmail.com>
#   Released under the same terms as Sensu (the MIT license); see LICENSE for details.
#

require 'sensu-plugin/check/cli'
require 'rest-client'
require 'json'

class CrateNodeStatus < Sensu::Plugin::Check::CLI
  check_name 'check-crate-node-status'

  option :host,
         description: 'Crate host',
         short: '-h HOST',
         long: '--host HOST',
         default: 'localhost'

  option :port,
         description: 'Crate port',
         short: '-p PORT',
         long: '--port PORT',
         proc: proc(&:to_i),
         default: 4200

  option :timeout,
         description: 'Sets the connection timeout for REST client',
         short: '-t SECS',
         long: '--timeout SECS',
         proc: proc(&:to_i),
         default: 30

  option :https,
         description: 'Enables HTTPS',
         short: '-e',
         long: '--https'

  def get_crate_status()
    headers = {:content_type=>:json}
    protocol = if config[:https]
                 'https'
               else
                 'http'
               end

    r = RestClient::Request.execute(method: :get,
                                    url: "#{protocol}://#{config[:host]}:#{config[:port]}",
                                    timeout: config[:timeout],
                                    headers: headers)
    JSON.parse(r)

  rescue Errno::ECONNREFUSED
    critical 'Connection refused'
  rescue RestClient::RequestTimeout
    critical 'Connection timed out'
  rescue Errno::ECONNRESET
    critical 'Connection reset by peer'
  end

  def acquire_status
    get_crate_status()
  end

  def run
    node_status = acquire_status

    if node_status['status'] == 200
      ok "Alive: #{node_status}"
    else
      critical "Dead: (#{node_status})"
    end
  end
end
