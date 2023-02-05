CREATE TABLE "primary_sheet"(id SERIAL PRIMARY KEY,
weekday TEXT NOT NULL,
date DATE NOT NULL,
ticker TEXT NOT NULL,
exchange TEXT NOT NULL,
industry TEXT NOT NULL,
so BIGINT NOT NULL,
dv BIGINT NOT NULL,
prior_day_vol BIGINT NOT NULL,
prior_day_vwap FLOAT NOT NULL,
b_run_up_low FLOAT NOT NULL,
run_up_high FLOAT NOT NULL,
prev_close FLOAT NOT NULL,
open FLOAT NOT NULL,
low FLOAT NOT NULL,
five_mins_high FLOAT NOT NULL,
high FLOAT NOT NULL,
close FLOAT NOT NULL,
vol_b4_bo BIGINT NOT NULL,
hod TEXT NOT NULL);

ALTER TABLE primary_sheet ADD COLUMN mkt_cap BIGINT GENERATED ALWAYS AS (so*b_run_up_low) STORED;

ALTER TABLE primary_sheet ADD COLUMN entry_on_bo_gain FLOAT GENERATED ALWAYS AS ((high-five_mins_high)/five_mins_high) STORED;

ALTER TABLE primary_sheet ADD COLUMN gap FLOAT GENERATED ALWAYS AS ((open-prev_close)/prev_close) STORED;

ALTER TABLE primary_sheet ADD COLUMN prior_day_dollar_vol BIGINT GENERATED ALWAYS AS (prior_day_vol*prior_day_vwap) STORED;

ALTER TABLE primary_sheet ADD COLUMN total_runup FLOAT GENERATED ALWAYS AS ((run_up_high-b_run_up_low)/b_run_up_low) STORED;

CREATE TABLE "one_min_data" (stock_one_min_id INT,
						  ticker TEXT NOT NULL,
						  date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
						  open FLOAT NOT NULL,
						  high FLOAT NOT NULL,
						  low FLOAT NOT NULL,
						  close FLOAT NOT NULL,
						  volume BIGINT NOT NULL,
						  vwap FLOAT NOT NULL,
					      n_of_trades INT,
						  FOREIGN KEY(stock_one_min_id) REFERENCES
						 primary_sheet (id));
