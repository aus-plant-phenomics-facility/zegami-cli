#docker run -it --network="host" -v $PWD/imageset.yaml:/config.yaml -v /plantdb/ftp-2019/0000_Production_N/:/images zegami-cli
zeg update imageset {imageset_id} --config imageset.yaml --project OVdSdE5n --token {token}
