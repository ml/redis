#!/usr/bin/env ruby

require 'rubygems'
require 'redis'
require 'benchmark'
require 'active_support'

$r = Redis.new :host => 'localhost', :db => "5"

def test(name, expected)
  $r.flushall
  setup
  result = yield

  expected = expected.call if expected.respond_to? :call
  puts "#{name} - " + if result == expected
    "passed"
  else
    "failed - expected #{expected.inspect} got #{result.inspect}"
  end
end

def setup
  $r.zadd "foo", 1, 1
  $r.zadd "foo", 2, 2
  $r.zadd "foo", 5, 5

  $r.zadd "bar", 3, 3
  $r.zadd "bar", 1, 1
  $r.zadd "bar", 4, 4

  $r.zadd "baz", 1, 1
  $r.zadd "baz", 3, 3
  $r.zadd "baz", 5, 5
  $r.zadd "baz", 8, 8


  $r.zadd "kiszonka", 4, 4
  $r.zadd "kiszonka", 1, 1
  $r.zadd "kiszonka", 6, 6
  $r.zadd "kiszonka", 3, 3
  $r.zadd "kiszonka", 7, 7

  $r.zadd "outofrange", 6, 6
  $r.zadd "outofrange", 7, 7

  $r.zadd "forinter", 1, 1
  $r.zadd "forinter", 5, 5

  (0..20).each {|i| $r.zadd "test1", i, i }
  (10..30).each { |i| $r.zadd "test2", i, i }
end


test("zrangeunion with offset 0", %w[1 2 3 4 5]) { $r.zrangeunion 0, 5, "foo", "bar", "baz" }

test("zrangeunion with offset", %w[2 3 4 5]) { $r.zrangeunion 1, 5, "foo", "bar", "baz" }

test("zrangeunion with range bigget than data set", %w[1 2 3 4 5 8]) { $r.zrangeunion 0, 20, "foo", "bar", "baz" }

test("zrevrangeunion with offset 0", %w[8 5 4 3 2]) { $r.zrevrangeunion 0, 5, "foo", "bar", "baz" }

test("zrevrange with offset", %w[4 3 2]) { $r.zrevrangeunion 2, 5, "foo", "bar", "baz" }

test("zrevrange with range bigger than data set", %w[8 5 4 3 2 1]) { $r.zrevrangeunion 0, 50, "foo", "bar", "baz" }

manual_union = lambda { %w[foo bar baz].map { |k| $r.zrange k, 0, 50 }.flatten.uniq.sort }.call

test("zunionstore", manual_union) { $r.zunionstore "dupa", *%w[foo bar baz]; $r.zrange "dupa", 0, 50 }

test("zdiffrange", %w[6 7]) { $r.zdiffrange 0, 50, *%w[kiszonka foo bar baz] }

test("zdiffrevrange", %w[7 6 4 3]) { $r.zdiffrevrange 0, 4, "kiszonka", "foo" }

test("zdiffrange", %w[1 3 4]) { $r.zdiffrange 0, 3, "kiszonka", "outofrange" }

test("zinterstore", %w[1 5]) { $r.zinterstore "intz0r", "foo", "baz", "forinter";  $r.zrange("intz0r", 0, 10) }

test("zinterstore with only two keys", (10..20).to_a.map { |i| i.to_s } ) {
  $r.zinterstore "intz0r2","test1", "test2";
  $r.zrange "intz0r2", 0, 10
}

#keys = %w[s dm ut n:to n:from tag:]
#
#keys.each do |key|
#  $r.pipelined do |r|
#   (0..5000).each { |i| r.zadd key, i, i }
#  end
#end
#
#puts Benchmark.ms { $r.zunionstore "dashboard", *keys }