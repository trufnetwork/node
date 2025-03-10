here we want to test the composition of streams and a way to get all substreams of a stream

suppose we have streams in this structure:

start time 0:
stream 1c:
- substream 1.1c
    - substream 1.1.1p
    - substream 1.1.2p
- substream 1.2c
    - substream 1.2.1p
- substream 1.3p
- substream 1.4c

start time 5:
stream 1c:
- substream 1.1c
    - substream 1.1.1p

start time 10:
stream 1c:
- substream 1.5p
- substream 1.6c (doesn't exist)

start time 6 but disabled:
stream 1c:
- substream 1.1c

so existing streams are:
- 1c
- 1.1c
- 1.1.1p
- 1.1.2p
- 1.2c
- 1.2.1p
- 1.3p
- 1.4c
- 1.5p