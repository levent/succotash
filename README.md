# Experiments with Storm and the Cosm firehose

Uses [RedStorm](https://github.com/colinsurprenant/redstorm)

## Install

1. Install gems locally

``` sh
$ bundle install
```


2. Install RedStorm

``` sh
$ bundle exec redstorm install
```

## Run

### Tag counter

Counts occurrences of tags in realtime.

Assumes you have a redis server running on localhost 6379.

1. Install topology gems

``` sh
$ bundle exec redstorm bundle cosm
```

2. Configure your Cosm api key

``` sh
$ export API_KEY=cosm_api_key
```

3. Run the topology!

``` sh
$ redstorm local cosm/message_count_topology.rb
```
