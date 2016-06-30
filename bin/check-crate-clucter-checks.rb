#! /usr/bin/env ruby
#
#  check-crate-clucter-checks
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
#   check-crate-clucter-checks --help
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

class CrateClusterChecks < Sensu::Plugin::Check::CLI
  check_name 'check-crate-clucter-checks'

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

  def get_crate_query(q)
    headers = {:content_type=>:json}
    protocol = if config[:https]
                 'https'
               else
                 'http'
               end

    r = RestClient::Request.execute(method: :post,
                                    url: "#{protocol}://#{config[:host]}:#{config[:port]}/_sql",
                                    timeout: config[:timeout],
                                    headers: headers,
                                    payload: {:stmt => "#{q}"}.to_json)
    JSON.parse(r)

  rescue Errno::ECONNREFUSED
    critical 'Connection refused'
  rescue RestClient::RequestTimeout
    critical 'Connection timed out'
  rescue Errno::ECONNRESET
    critical 'Connection reset by peer'
  end

  def acquire_status
    get_crate_query('SELECT description FROM sys.checks WHERE passed = false ORDER BY severity')
  end

  def run
    info = acquire_status

    if info['rowcount'] == 0
      ok "#{info['rowcount']} Checks Failing"
    else
      critical "#{info['rowcount']} Checks Failed: #{info['rows']} "
    end
  end
end
