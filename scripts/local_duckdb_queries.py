import duckdb

con = duckdb.connect("weekly_streamer_test")
con.sql("SELECT * FROM channel_stats_test").show()