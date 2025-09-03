time to insert each 100K rows of `pending_prune_days`: ~3 seconds
considering we have 29_500_000 rows, this would take around 15 minutes of non-stop inserting. 

time to process 625 pending_prune_days rows: ~18 seconds
considering we have 29_500_000 rows, this would take around 10 days of non-stop processing.

making the network busy only 5% of the time: 10 days * 20 = 200 days to complete the migration