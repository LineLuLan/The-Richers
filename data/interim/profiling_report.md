# Data Profiling Report

Generated at: 2026-04-30 15:29:07.316952


## 1. Loaded 13 tables

| Table | Rows | Cols | Memory (MB) |
|-------|------|------|-------------|
| products | 2,412 | 8 | 0.7 |
| customers | 121,930 | 7 | 35.0 |
| promotions | 50 | 10 | 0.0 |
| geography | 39,948 | 4 | 6.9 |
| orders | 646,945 | 8 | 194.2 |
| order_items | 714,669 | 7 | 78.0 |
| payments | 646,945 | 4 | 50.6 |
| shipments | 566,067 | 4 | 72.3 |
| returns | 39,939 | 7 | 8.0 |
| reviews | 113,551 | 7 | 23.3 |
| sales | 3,833 | 3 | 0.3 |
| inventory | 60,247 | 17 | 19.7 |
| web_traffic | 3,652 | 7 | 0.6 |

## 2. Schema & Dtypes


### `products.csv`
```
product_id        int64
product_name     object
category         object
segment          object
size             object
color            object
price           float64
cogs            float64
```

### `customers.csv`
```
customer_id             int64
zip                     int64
city                   object
signup_date            object
gender                 object
age_group              object
acquisition_channel    object
```
⚠️  Cột tên có "date" nhưng dtype object: ['signup_date']
   signup_date sample: ['2021-12-30', '2013-12-27', '2018-07-24']

### `promotions.csv`
```
promo_id                object
promo_name              object
promo_type              object
discount_value         float64
start_date              object
end_date                object
applicable_category     object
promo_channel           object
stackable_flag           int64
min_order_value          int64
```
⚠️  Cột tên có "date" nhưng dtype object: ['start_date', 'end_date']
   start_date sample: ['2013-03-18', '2013-06-23', '2013-08-30']
   end_date sample: ['2013-04-17', '2013-07-22', '2013-10-02']

### `geography.csv`
```
zip          int64
city        object
region      object
district    object
```

### `orders.csv`
```
order_id           int64
order_date        object
customer_id        int64
zip                int64
order_status      object
payment_method    object
device_type       object
order_source      object
```
⚠️  Cột tên có "date" nhưng dtype object: ['order_date']
   order_date sample: ['2012-07-04', '2012-07-04', '2012-07-04']

### `order_items.csv`
```
order_id             int64
product_id           int64
quantity             int64
unit_price         float64
discount_amount    float64
promo_id            object
promo_id_2          object
```

### `payments.csv`
```
order_id            int64
payment_method     object
payment_value     float64
installments        int64
```

### `shipments.csv`
```
order_id           int64
ship_date         object
delivery_date     object
shipping_fee     float64
```
⚠️  Cột tên có "date" nhưng dtype object: ['ship_date', 'delivery_date']
   ship_date sample: ['2012-07-07', '2012-07-06', '2012-07-04']
   delivery_date sample: ['2012-07-11', '2012-07-10', '2012-07-07']

### `returns.csv`
```
return_id           object
order_id             int64
product_id           int64
return_date         object
return_reason       object
return_quantity      int64
refund_amount      float64
```
⚠️  Cột tên có "date" nhưng dtype object: ['return_date']
   return_date sample: ['2012-07-25', '2012-07-16', '2012-07-16']

### `reviews.csv`
```
review_id       object
order_id         int64
product_id       int64
customer_id      int64
review_date     object
rating           int64
review_title    object
```
⚠️  Cột tên có "date" nhưng dtype object: ['review_date']
   review_date sample: ['2012-07-24', '2012-08-03', '2012-07-23']

### `sales.csv`
```
Date        object
Revenue    float64
COGS       float64
```
⚠️  Cột tên có "date" nhưng dtype object: ['Date']
   Date sample: ['2012-07-04', '2012-07-05', '2012-07-06']

### `inventory.csv`
```
snapshot_date         object
product_id             int64
stock_on_hand          int64
units_received         int64
units_sold             int64
stockout_days          int64
days_of_supply       float64
fill_rate            float64
stockout_flag          int64
overstock_flag         int64
reorder_flag           int64
sell_through_rate    float64
product_name          object
category              object
segment               object
year                   int64
month                  int64
```
⚠️  Cột tên có "date" nhưng dtype object: ['snapshot_date']
   snapshot_date sample: ['2022-10-31', '2022-11-30', '2022-12-31']

### `web_traffic.csv`
```
date                         object
sessions                      int64
unique_visitors               int64
page_views                    int64
bounce_rate                 float64
avg_session_duration_sec    float64
traffic_source               object
```
⚠️  Cột tên có "date" nhưng dtype object: ['date']
   date sample: ['2013-01-01', '2013-01-02', '2013-01-03']

## 3. Missing Values


### `products` — không có missing nào ✓

### `customers` — không có missing nào ✓

### `promotions` — 1 cột có missing:
```
                     n_missing  pct_missing
applicable_category         40         80.0
```

### `geography` — không có missing nào ✓

### `orders` — không có missing nào ✓

### `order_items` — 2 cột có missing:
```
            n_missing  pct_missing
promo_id_2     714463        99.97
promo_id       438353        61.34
```

### `payments` — không có missing nào ✓

### `shipments` — không có missing nào ✓

### `returns` — không có missing nào ✓

### `reviews` — không có missing nào ✓

### `sales` — không có missing nào ✓

### `inventory` — không có missing nào ✓

### `web_traffic` — không có missing nào ✓

## 4. Categorical Values (unique <= 50)


### `products`

#### `products.category` — 4 unique values:
```
category
Streetwear    1320
Outdoor        743
Casual         201
GenZ           148
```

#### `products.segment` — 8 unique values:
```
segment
Activewear     598
Everyday       405
Performance    347
Balanced       306
Standard       262
Premium        177
All-weather    169
Trendy         148
```

#### `products.size` — 4 unique values:
```
size
S     603
M     603
L     603
XL    603
```

#### `products.color` — 10 unique values:
```
color
orange    242
black     242
silver    241
green     241
yellow    241
pink      241
red       241
blue      241
white     241
purple    241
```

### `customers`

#### `customers.city` — 42 unique values:
```
city
Cam Pha                4398
Thai Nguyen            4347
Phu Ly                 4243
Hanoi                  4240
Ha Long                4236
Bac Ninh               4172
Hai Phong              4170
Nam Dinh               4169
Bac Giang              4160
Ninh Binh              4081
Son Tay                4075
Viet Tri               4054
Uong Bi                4026
Dong Hoi               3912
Kon Tum                3838
Lao Cai                3807
Hoi An                 3760
Phan Thiet             3749
Phan Rang-Thap Cham    3734
Hue                    3719
Tuy Hoa                3674
Quang Ngai             3616
Da Nang                3616
Tam Ky                 3562
Quy Nhon               3556
Nha Trang              3550
Ho Chi Minh City       1359
Vinh Long              1336
Pleiku                 1253
Vung Tau               1251
Da Lat                 1243
Can Tho                1214
Ca Mau                 1211
Bac Lieu               1211
Soc Trang              1208
My Tho                 1198
Long Xuyen             1194
Ben Tre                1179
Bien Hoa               1173
Buon Ma Thuot          1167
Tra Vinh               1138
Rach Gia               1131
```

#### `customers.gender` — 3 unique values:
```
gender
Female        59640
Male          57457
Non-binary     4833
```

#### `customers.age_group` — 5 unique values:
```
age_group
25-34    36342
35-44    31920
45-54    23172
18-24    17039
55+      13457
```

#### `customers.acquisition_channel` — 6 unique values:
```
acquisition_channel
organic_search    36450
social_media      24448
paid_search       24285
email_campaign    14674
referral          12270
direct             9803
```

### `promotions`

#### `promotions.promo_id` — 50 unique values:
```
promo_id
PROMO-0001    1
PROMO-0002    1
PROMO-0003    1
PROMO-0004    1
PROMO-0005    1
PROMO-0006    1
PROMO-0007    1
PROMO-0008    1
PROMO-0009    1
PROMO-0010    1
PROMO-0011    1
PROMO-0012    1
PROMO-0013    1
PROMO-0014    1
PROMO-0015    1
PROMO-0016    1
PROMO-0017    1
PROMO-0018    1
PROMO-0019    1
PROMO-0020    1
PROMO-0021    1
PROMO-0022    1
PROMO-0023    1
PROMO-0024    1
PROMO-0025    1
PROMO-0026    1
PROMO-0027    1
PROMO-0028    1
PROMO-0029    1
PROMO-0030    1
PROMO-0031    1
PROMO-0032    1
PROMO-0033    1
PROMO-0034    1
PROMO-0035    1
PROMO-0036    1
PROMO-0037    1
PROMO-0038    1
PROMO-0039    1
PROMO-0040    1
PROMO-0041    1
PROMO-0042    1
PROMO-0043    1
PROMO-0044    1
PROMO-0045    1
PROMO-0046    1
PROMO-0047    1
PROMO-0048    1
PROMO-0049    1
PROMO-0050    1
```

#### `promotions.promo_name` — 50 unique values:
```
promo_name
Spring Sale 2013      1
Mid-Year Sale 2013    1
Fall Launch 2013      1
Year-End Sale 2013    1
Urban Blowout 2013    1
Rural Special 2013    1
Spring Sale 2014      1
Mid-Year Sale 2014    1
Fall Launch 2014      1
Year-End Sale 2014    1
Spring Sale 2015      1
Mid-Year Sale 2015    1
Fall Launch 2015      1
Year-End Sale 2015    1
Urban Blowout 2015    1
Rural Special 2015    1
Spring Sale 2016      1
Mid-Year Sale 2016    1
Fall Launch 2016      1
Year-End Sale 2016    1
Spring Sale 2017      1
Mid-Year Sale 2017    1
Fall Launch 2017      1
Year-End Sale 2017    1
Urban Blowout 2017    1
Rural Special 2017    1
Spring Sale 2018      1
Mid-Year Sale 2018    1
Fall Launch 2018      1
Year-End Sale 2018    1
Spring Sale 2019      1
Mid-Year Sale 2019    1
Fall Launch 2019      1
Year-End Sale 2019    1
Urban Blowout 2019    1
Rural Special 2019    1
Spring Sale 2020      1
Mid-Year Sale 2020    1
Fall Launch 2020      1
Year-End Sale 2020    1
Spring Sale 2021      1
Mid-Year Sale 2021    1
Fall Launch 2021      1
Year-End Sale 2021    1
Urban Blowout 2021    1
Rural Special 2021    1
Spring Sale 2022      1
Mid-Year Sale 2022    1
Fall Launch 2022      1
Year-End Sale 2022    1
```

#### `promotions.promo_type` — 2 unique values:
```
promo_type
percentage    45
fixed          5
```

#### `promotions.start_date` — 50 unique values:
```
start_date
2013-03-18    1
2013-06-23    1
2013-08-30    1
2013-11-18    1
2013-07-30    1
2013-01-31    1
2014-03-18    1
2014-06-23    1
2014-08-30    1
2014-11-19    1
2015-03-18    1
2015-06-23    1
2015-08-30    1
2015-11-18    1
2015-07-30    1
2015-01-30    1
2016-03-18    1
2016-06-23    1
2016-08-30    1
2016-11-18    1
2017-03-18    1
2017-06-23    1
2017-08-30    1
2017-11-18    1
2017-07-30    1
2017-01-30    1
2018-03-18    1
2018-06-23    1
2018-08-30    1
2018-11-18    1
2019-03-18    1
2019-06-23    1
2019-08-30    1
2019-11-18    1
2019-07-30    1
2019-01-30    1
2020-03-18    1
2020-06-23    1
2020-08-30    1
2020-11-18    1
2021-03-18    1
2021-06-23    1
2021-08-30    1
2021-11-18    1
2021-07-30    1
2021-01-30    1
2022-03-18    1
2022-06-23    1
2022-08-31    1
2022-11-18    1
```

#### `promotions.end_date` — 50 unique values:
```
end_date
2013-04-17    1
2013-07-22    1
2013-10-02    1
2014-01-02    1
2013-09-02    1
2013-03-01    1
2014-04-17    1
2014-07-22    1
2014-10-01    1
2015-01-02    1
2015-04-17    1
2015-07-22    1
2015-10-01    1
2016-01-02    1
2015-09-02    1
2015-03-01    1
2016-04-17    1
2016-07-22    1
2016-10-01    1
2017-01-02    1
2017-04-17    1
2017-07-22    1
2017-10-02    1
2018-01-02    1
2017-09-02    1
2017-03-01    1
2018-04-17    1
2018-07-22    1
2018-10-01    1
2019-01-02    1
2019-04-17    1
2019-07-22    1
2019-10-01    1
2020-01-02    1
2019-09-02    1
2019-03-01    1
2020-04-17    1
2020-07-22    1
2020-10-01    1
2021-01-01    1
2021-04-17    1
2021-07-22    1
2021-10-02    1
2022-01-02    1
2021-09-02    1
2021-03-01    1
2022-04-17    1
2022-07-22    1
2022-10-01    1
2022-12-31    1
```

#### `promotions.applicable_category` — 2 unique values:
```
applicable_category
NaN           40
Streetwear     5
Outdoor        5
```

#### `promotions.promo_channel` — 5 unique values:
```
promo_channel
all_channels    19
online          13
email            7
social_media     6
in_store         5
```

### `geography`

#### `geography.city` — 42 unique values:
```
city
Cam Pha                1403
Phu Ly                 1399
Thai Nguyen            1394
Hanoi                  1376
Nam Dinh               1370
Ha Long                1357
Bac Giang              1347
Hai Phong              1346
Bac Ninh               1346
Son Tay                1344
Ninh Binh              1326
Uong Bi                1325
Viet Tri               1324
Lao Cai                1272
Kon Tum                1265
Hoi An                 1255
Dong Hoi               1246
Phan Rang-Thap Cham    1219
Hue                    1216
Tuy Hoa                1212
Nha Trang              1199
Quang Ngai             1192
Phan Thiet             1189
Tam Ky                 1180
Da Nang                1172
Quy Nhon               1167
Ho Chi Minh City        456
Vinh Long               431
Vung Tau                419
Ca Mau                  416
Pleiku                  414
Long Xuyen              408
My Tho                  408
Da Lat                  408
Buon Ma Thuot           401
Bien Hoa                399
Soc Trang               396
Can Tho                 394
Bac Lieu                393
Rach Gia                392
Tra Vinh                391
Ben Tre                 381
```

#### `geography.region` — 3 unique values:
```
region
East       18929
Central    14512
West        6507
```

#### `geography.district` — 39 unique values:
```
district
District #25    1597
District #30    1518
District #19    1513
District #24    1511
District #07    1492
District #21    1459
District #32    1459
District #01    1337
District #33    1326
District #23    1210
District #03    1148
District #39    1113
District #15    1095
District #13    1083
District #29    1059
District #22    1056
District #09    1044
District #11    1033
District #02    1010
District #18     986
District #34     983
District #28     966
District #16     963
District #04     908
District #35     850
District #10     840
District #17     837
District #05     797
District #37     796
District #36     779
District #27     771
District #08     765
District #06     749
District #20     725
District #12     691
District #38     660
District #26     644
District #14     638
District #31     537
```

### `orders`

#### `orders.order_status` — 6 unique values:
```
order_status
delivered    516716
cancelled     59462
returned      36142
shipped       13773
paid          13577
created        7275
```

#### `orders.payment_method` — 5 unique values:
```
payment_method
credit_card      356352
paypal            97018
cod               96681
apple_pay         64763
bank_transfer     32131
```

#### `orders.device_type` — 3 unique values:
```
device_type
mobile     291482
desktop    258855
tablet      96608
```

#### `orders.order_source` — 6 unique values:
```
order_source
organic_search    181495
paid_search       141652
social_media      129710
email_campaign     77572
referral           64565
direct             51951
```

### `order_items`

#### `order_items.promo_id` — 50 unique values:
```
promo_id
NaN           438353
PROMO-0014     11451
PROMO-0010     11345
PROMO-0004     11126
PROMO-0020     10121
PROMO-0011      9594
PROMO-0007      9373
PROMO-0021      8966
PROMO-0017      8808
PROMO-0001      8523
PROMO-0024      8205
PROMO-0028      8146
PROMO-0002      7754
PROMO-0027      7518
PROMO-0022      7506
PROMO-0013      7498
PROMO-0018      7089
PROMO-0008      7089
PROMO-0012      6802
PROMO-0003      6277
PROMO-0019      5763
PROMO-0009      5637
PROMO-0023      5413
PROMO-0025      5385
PROMO-0015      5072
PROMO-0030      5026
PROMO-0005      4887
PROMO-0047      4791
PROMO-0031      4718
PROMO-0037      4661
PROMO-0041      4639
PROMO-0029      4328
PROMO-0034      4277
PROMO-0032      3867
PROMO-0050      3500
PROMO-0044      3462
PROMO-0040      3286
PROMO-0035      3174
PROMO-0033      3101
PROMO-0048      3091
PROMO-0038      3074
PROMO-0042      2999
PROMO-0006      2828
PROMO-0039      2593
PROMO-0049      2568
PROMO-0016      2526
PROMO-0043      2476
PROMO-0045      2432
PROMO-0026      1881
PROMO-0036      1082
PROMO-0046       588
```

#### `order_items.promo_id_2` — 2 unique values:
```
promo_id_2
NaN           714463
PROMO-0015       132
PROMO-0025        74
```

### `payments`

#### `payments.payment_method` — 5 unique values:
```
payment_method
credit_card      356352
paypal            97018
cod               96681
apple_pay         64763
bank_transfer     32131
```

### `returns`

#### `returns.return_reason` — 5 unique values:
```
return_reason
wrong_size          13967
defective            8020
not_as_described     7035
changed_mind         6931
late_delivery        3986
```

### `reviews`

#### `reviews.review_title` — 18 unique values:
```
review_title
Very satisfied             11450
Highly recommend           11407
Great quality              11218
Excellent product!         11181
Good overall                9185
Happy with purchase         9171
Solid choice                9070
Works well                  8986
Mixed feelings              5706
Average product             5656
Decent, nothing special     5654
Some issues                 3037
Would not reorder           3034
Below expectations          3024
Would not recommend         1460
Poor quality                1443
Very disappointed           1442
Not as described            1427
```

### `inventory`

#### `inventory.category` — 4 unique values:
```
category
Streetwear    31020
Outdoor       21050
GenZ           4674
Casual         3503
```

#### `inventory.segment` — 8 unique values:
```
segment
Activewear     18290
Everyday       13598
Performance     7673
Balanced        6622
Trendy          4674
Premium         3182
Standard        3127
All-weather     3081
```

### `web_traffic`

#### `web_traffic.traffic_source` — 6 unique values:
```
traffic_source
organic_search    1090
paid_search        784
social_media       632
email_campaign     505
referral           375
direct             266
```

## 5. High-cardinality string columns


### `products`

#### `products.product_name` — 2,172 unique, 1,986 singletons (xuất hiện đúng 1 lần)
Top 10:
```
product_name
VietMode RP-02    3
VietMode RP-03    3
VietMode RP-04    3
VietMode RP-05    3
VietMode RP-06    3
VietMode RP-07    3
VietMode RP-08    3
VietMode RP-09    3
VietMode RP-10    3
VietMode RP-11    3
```
Bottom 10 (có thể là typo):
```
product_name
HanoiStreet UC-42    1
HanoiStreet UC-43    1
HanoiStreet UC-44    1
HanoiStreet UC-45    1
HanoiStreet UC-46    1
HanoiStreet UC-47    1
HanoiStreet UC-48    1
HanoiStreet UC-49    1
HanoiStreet UC-50    1
LotusWear RP-03      1
```

### `customers`

#### `customers.signup_date` — 3,941 unique, 53 singletons (xuất hiện đúng 1 lần)
Top 10:
```
signup_date
2022-06-02    78
2022-11-13    77
2022-11-15    76
2021-10-31    75
2022-07-18    75
2022-11-30    74
2022-10-25    73
2022-06-28    73
2022-11-22    73
2021-10-05    72
```
Bottom 10 (có thể là typo):
```
signup_date
2012-03-01    1
2012-09-08    1
2012-03-14    1
2012-04-20    1
2012-04-11    1
2012-06-14    1
2012-05-23    1
2012-09-18    1
2012-06-25    1
2012-05-09    1
```

### `orders`

#### `orders.order_date` — 3,833 unique, 0 singletons (xuất hiện đúng 1 lần)
Top 10:
```
order_date
2018-05-30    803
2018-05-31    782
2018-06-01    725
2017-03-30    725
2014-04-29    700
2016-04-28    677
2017-06-01    657
2017-03-31    656
2016-04-29    655
2016-03-31    654
```
Bottom 10 (có thể là typo):
```
order_date
2021-12-03    22
2020-12-02    20
2020-10-02    19
2022-01-03    19
2020-03-01    19
2021-01-02    17
2021-03-02    14
2020-01-02    13
2013-03-02     9
2020-02-29     8
```

### `shipments`

#### `shipments.ship_date` — 3,831 unique, 0 singletons (xuất hiện đúng 1 lần)
Top 10:
```
ship_date
2018-06-02    678
2017-06-03    592
2018-06-03    588
2018-06-01    579
2017-06-02    554
2017-04-01    544
2017-04-02    542
2016-04-30    531
2016-06-03    531
2016-04-03    520
```
Bottom 10 (có thể là typo):
```
ship_date
2020-01-09    23
2020-02-05    23
2021-01-08    22
2021-01-04    22
2020-12-04    22
2021-01-16    21
2021-01-05    21
2021-01-09    20
2022-12-29    20
2020-03-03    11
```

#### `shipments.delivery_date` — 3,831 unique, 0 singletons (xuất hiện đúng 1 lần)
Top 10:
```
delivery_date
2018-06-06    562
2018-06-07    506
2018-06-08    484
2016-05-05    482
2016-05-04    470
2018-06-05    469
2017-06-07    469
2014-06-05    458
2017-04-07    456
2014-06-04    456
```
Bottom 10 (có thể là typo):
```
delivery_date
2022-01-13    25
2020-01-12    25
2021-01-18    25
2020-12-11    24
2020-12-10    24
2021-01-11    23
2021-01-12    21
2021-01-16    19
2012-07-07    15
2012-07-06     6
```

### `returns`

#### `returns.return_id` — 39,939 unique, 39,939 singletons (xuất hiện đúng 1 lần)
Top 10:
```
return_id
RET-051504    1
RET-000001    1
RET-000002    1
RET-051419    1
RET-051418    1
RET-051417    1
RET-051416    1
RET-051412    1
RET-051408    1
RET-051406    1
```
Bottom 10 (có thể là typo):
```
return_id
RET-000015    1
RET-000014    1
RET-000013    1
RET-000012    1
RET-000010    1
RET-000008    1
RET-000007    1
RET-000006    1
RET-000005    1
RET-000004    1
```

#### `returns.return_date` — 3,806 unique, 61 singletons (xuất hiện đúng 1 lần)
Top 10:
```
return_date
2016-06-18    34
2014-04-17    33
2018-06-15    33
2013-06-19    33
2016-04-19    33
2016-04-24    32
2015-06-08    31
2015-09-14    31
2013-06-24    31
2016-05-17    31
```
Bottom 10 (có thể là typo):
```
return_date
2020-02-13    1
2021-03-06    1
2020-01-03    1
2022-12-15    1
2022-11-09    1
2020-01-24    1
2022-11-15    1
2022-11-12    1
2012-07-12    1
2012-07-11    1
```

### `reviews`

#### `reviews.review_id` — 113,551 unique, 113,551 singletons (xuất hiện đúng 1 lần)
Top 10:
```
review_id
REV-0146984    1
REV-0000001    1
REV-0000002    1
REV-0000003    1
REV-0000005    1
REV-0000006    1
REV-0000008    1
REV-0000009    1
REV-0000010    1
REV-0000011    1
```
Bottom 10 (có thể là typo):
```
review_id
REV-0000033    1
REV-0000032    1
REV-0000030    1
REV-0000028    1
REV-0000027    1
REV-0000026    1
REV-0000025    1
REV-0000024    1
REV-0000023    1
REV-0000022    1
```

#### `reviews.review_date` — 3,825 unique, 2 singletons (xuất hiện đúng 1 lần)
Top 10:
```
review_date
2017-05-06    77
2014-05-11    76
2017-06-19    76
2017-06-14    76
2015-05-10    75
2016-06-07    75
2016-05-16    73
2016-06-12    73
2014-05-25    72
2015-04-28    72
```
Bottom 10 (có thể là typo):
```
review_date
2021-12-13    4
2021-11-24    4
2012-07-16    4
2021-03-01    4
2021-02-24    3
2022-02-23    2
2012-07-13    2
2012-07-14    2
2012-07-12    1
2012-07-10    1
```

### `sales`

#### `sales.Date` — 3,833 unique, 3,833 singletons (xuất hiện đúng 1 lần)
Top 10:
```
Date
2022-12-31    1
2012-07-04    1
2012-07-05    1
2012-07-06    1
2012-07-07    1
2012-07-08    1
2012-07-09    1
2012-07-10    1
2012-07-11    1
2022-12-15    1
```
Bottom 10 (có thể là typo):
```
Date
2012-07-22    1
2012-07-21    1
2012-07-20    1
2012-07-19    1
2012-07-18    1
2012-07-17    1
2012-07-16    1
2012-07-15    1
2012-07-14    1
2012-07-13    1
```

### `inventory`

#### `inventory.snapshot_date` — 126 unique, 0 singletons (xuất hiện đúng 1 lần)
Top 10:
```
snapshot_date
2018-05-31    562
2021-03-31    551
2018-06-30    550
2018-04-30    550
2021-04-30    548
2020-08-31    547
2020-04-30    546
2017-08-31    546
2020-05-31    542
2019-08-31    532
```
Bottom 10 (có thể là typo):
```
snapshot_date
2012-11-30    411
2022-07-31    410
2022-10-31    409
2020-11-30    402
2012-07-31    395
2021-11-30    389
2022-01-31    388
2020-01-31    383
2021-01-31    377
2022-11-30    376
```

#### `inventory.product_name` — 1,465 unique, 95 singletons (xuất hiện đúng 1 lần)
Top 10:
```
product_name
VietMode RP-10      203
VietMode RP-09      203
VietMode RP-11      190
VietMode RP-12      190
VietMode RP-34      170
VietMode RP-33      170
VietMode RP-36      166
VietMode RP-35      166
SaigonFlex UC-55    153
VietMode RP-84      140
```
Bottom 10 (có thể là typo):
```
product_name
DragonWear UM-15    1
DragonWear UM-26    1
DragonWear UM-52    1
VietMotion RP-43    1
SaigonCore UC-08    1
VietMotion UC-20    1
VietMotion UR-28    1
VietMotion UR-10    1
VietMotion UE-02    1
VietMode MA-30      1
```

### `web_traffic`

#### `web_traffic.date` — 3,652 unique, 3,652 singletons (xuất hiện đúng 1 lần)
Top 10:
```
date
2022-12-31    1
2013-01-01    1
2013-01-02    1
2013-01-03    1
2022-12-15    1
2022-12-14    1
2022-12-13    1
2022-12-12    1
2022-12-11    1
2022-12-10    1
```
Bottom 10 (có thể là typo):
```
date
2013-01-14    1
2013-01-13    1
2013-01-12    1
2013-01-11    1
2013-01-10    1
2013-01-09    1
2013-01-08    1
2013-01-07    1
2013-01-06    1
2013-01-05    1
```

## 6. Numeric Columns Summary


### `products`
```
             count     mean      std   min     25%      50%      75%      max  n_zeros  n_negative
product_id  2412.0  1206.50   696.43  1.00  603.75  1206.50  1809.25   2412.0        0           0
price       2412.0  4928.22  4776.74  9.06   59.44  4399.60  7720.51  40950.0        0           0
cogs        2412.0  3868.35  3878.58  5.18   35.07  3184.93  5864.92  38902.5        0           0
```

### `customers`
```
                count      mean       std     min       25%      50%        75%       max  n_zeros  n_negative
customer_id  121930.0  78736.90  45492.20     1.0  39343.50  78784.5  118156.75  157563.0        0           0
zip          121930.0  50990.17  26871.91  1001.0  28689.25  49835.0   73488.00   99950.0        0           0
```

### `promotions`
```
                 count      mean       std   min   25%   50%       75%       max  n_zeros  n_negative
discount_value    50.0     18.50     11.24  10.0  12.0  16.5      20.0      50.0        0           0
stackable_flag    50.0      0.24      0.43   0.0   0.0   0.0       0.0       1.0       38           0
min_order_value   50.0  46000.00  66116.78   0.0   0.0   0.0  100000.0  200000.0       31           0
```

### `geography`
```
       count      mean       std  min      25%      50%       75%      max  n_zeros  n_negative
zip  39948.0  50895.08  27042.26  1.0  28279.5  49876.5  73526.25  99950.0        0           0
```

### `orders`
```
                count       mean        std     min       25%       50%       75%       max  n_zeros  n_negative
order_id     646945.0  417189.47  240785.70     1.0  208728.0  417211.0  625628.0  834397.0        0           0
customer_id  646945.0   84906.20   48446.92     1.0   41336.0   87279.0  133282.0  157563.0        0           0
zip          646945.0   55410.74   28876.47  1001.0   30904.0   54129.0   83301.0   99950.0        0           0
```

### `order_items`
```
                    count       mean        std     min        25%        50%        75%        max  n_zeros  n_negative
order_id         714669.0  411615.08  240480.31    1.00  203229.00  409306.00  618981.00  834397.00        0           0
product_id       714669.0    1234.93     691.33    1.00     689.00     990.00    2045.00    2412.00        0           0
quantity         714669.0       4.50       2.29    1.00       2.00       4.00       6.00       8.00        0           0
unit_price       714669.0    5114.69    3774.82  392.57    1906.89    4257.77    7273.76   43056.00        0           0
discount_amount  714669.0    1048.89    2280.53    0.00       0.00       0.00     967.63   35235.47   438353           0
```

### `payments`
```
                  count       mean        std     min        25%        50%        75%       max  n_zeros  n_negative
order_id       646945.0  417189.47  240785.70    1.00  208728.00  417211.00  625628.00  834397.0        0           0
payment_value  646945.0   24238.33   22378.48  389.74    7681.06   17229.44   33706.35  331570.4        0           0
installments   646945.0       3.45       3.12    1.00       1.00       3.00       6.00      12.0        0           0
```

### `shipments`
```
                 count       mean        std  min        25%        50%       75%       max  n_zeros  n_negative
order_id      566067.0  415816.87  240007.31  1.0  208192.50  415866.00  623218.5  834325.0        0           0
shipping_fee  566067.0       4.96       8.89  0.0       0.87       1.73       2.6      32.0      805           0
```

### `returns`
```
                   count       mean        std     min       25%        50%        75%        max  n_zeros  n_negative
order_id         39939.0  409061.98  240063.90    2.00  202651.0  404254.00  615620.00  833351.00        0           0
product_id       39939.0    1244.23     691.75    3.00     702.0     992.00    2048.00    2412.00        0           0
return_quantity  39939.0       2.74       1.83    1.00       1.0       2.00       4.00       8.00        0           0
refund_amount    39939.0   12784.46   14092.15  458.81    3573.4    7888.88   16881.99  160937.94        0           0
```

### `reviews`
```
                count       mean        std  min       25%       50%       75%       max  n_zeros  n_negative
order_id     113551.0  408999.52  239021.92  1.0  202048.5  406841.0  614844.0  833296.0        0           0
product_id   113551.0    1232.02     690.84  3.0     689.0     981.0    2045.0    2412.0        0           0
customer_id  113551.0   85694.34   48501.48  2.0   42096.0   89755.0  133850.0  157563.0        0           0
rating       113551.0       3.94       1.15  1.0       3.0       4.0       5.0       5.0        0           0
```

### `sales`
```
          count        mean         std        min         25%         50%         75%          max  n_zeros  n_negative
Revenue  3833.0  4286584.03  2624840.20  279813.94  2471088.82  3647303.90  5350877.20  20905271.35        0           0
COGS     3833.0  3695134.49  2219788.77  236576.31  2150580.23  3161112.99  4637293.92  16535857.67        0           0
```

### `inventory`
```
                     count     mean      std      min      25%      50%      75%       max  n_zeros  n_negative
product_id         60247.0  1311.41   673.05     1.00   760.00  1223.00  1942.00   2412.00        0           0
stock_on_hand      60247.0   189.30   316.98     3.00    15.00    62.00   210.00   2673.00        0           0
units_received     60247.0    18.05    34.08     1.00     2.00     6.00    19.00    817.00        0           0
units_sold         60247.0    15.42    28.40     1.00     2.00     6.00    16.00    670.00        0           0
stockout_days      60247.0     1.16     1.62     0.00     0.00     1.00     2.00     28.00    19676           0
days_of_supply     60247.0   912.68  2587.62     5.20    96.00   240.00   683.10  68100.00        0           0
fill_rate          60247.0     0.96     0.05     0.07     0.93     0.97     1.00      1.00        0           0
stockout_flag      60247.0     0.67     0.47     0.00     0.00     1.00     1.00      1.00    19676           0
overstock_flag     60247.0     0.76     0.43     0.00     1.00     1.00     1.00      1.00    14305           0
reorder_flag       60247.0     0.00     0.00     0.00     0.00     0.00     0.00      0.00    60247           0
sell_through_rate  60247.0     0.15     0.14     0.00     0.04     0.11     0.24      0.85        0           0
year               60247.0  2017.22     2.97  2012.00  2015.00  2017.00  2020.00   2022.00        0           0
month              60247.0     6.62     3.39     1.00     4.00     7.00    10.00     12.00        0           0
```

### `web_traffic`
```
                           count       mean       std      min       25%       50%        75%        max  n_zeros  n_negative
sessions                  3652.0   25041.77   9422.61   7973.0  17099.25   23633.5   31782.75   50947.00        0           0
unique_visitors           3652.0   19031.40   7237.95   6136.0  12915.00   17924.0   24191.75   40430.00        0           0
page_views                3652.0  108615.22  44472.06  30451.0  72982.00  101010.5  138086.00  275560.00        0           0
bounce_rate               3652.0       0.00      0.00      0.0      0.00       0.0       0.01       0.01        0           0
avg_session_duration_sec  3652.0     210.28     63.77    100.1    156.70     209.2     266.20     319.90        0           0
```

## 7. Date Columns


### `customers`

#### `customers.signup_date`
- Range: 2012-01-17 00:00:00 → 2022-12-31 00:00:00
- Distinct dates: 3,941
- Failed to parse: 0

### `promotions`

#### `promotions.start_date`
- Range: 2013-01-31 00:00:00 → 2022-11-18 00:00:00
- Distinct dates: 50
- Failed to parse: 0

#### `promotions.end_date`
- Range: 2013-03-01 00:00:00 → 2022-12-31 00:00:00
- Distinct dates: 50
- Failed to parse: 0

### `orders`

#### `orders.order_date`
- Range: 2012-07-04 00:00:00 → 2022-12-31 00:00:00
- Distinct dates: 3,833
- Failed to parse: 0

### `shipments`

#### `shipments.ship_date`
- Range: 2012-07-04 00:00:00 → 2022-12-29 00:00:00
- Distinct dates: 3,831
- Failed to parse: 0

#### `shipments.delivery_date`
- Range: 2012-07-06 00:00:00 → 2022-12-31 00:00:00
- Distinct dates: 3,831
- Failed to parse: 0

### `returns`

#### `returns.return_date`
- Range: 2012-07-11 00:00:00 → 2022-12-31 00:00:00
- Distinct dates: 3,806
- Failed to parse: 0

### `reviews`

#### `reviews.review_date`
- Range: 2012-07-10 00:00:00 → 2022-12-31 00:00:00
- Distinct dates: 3,825
- Failed to parse: 0

### `sales`

#### `sales.Date`
- Range: 2012-07-04 00:00:00 → 2022-12-31 00:00:00
- Distinct dates: 3,833
- Failed to parse: 0

### `inventory`

#### `inventory.snapshot_date`
- Range: 2012-07-31 00:00:00 → 2022-12-31 00:00:00
- Distinct dates: 126
- Failed to parse: 0

### `web_traffic`

#### `web_traffic.date`
- Range: 2013-01-01 00:00:00 → 2022-12-31 00:00:00
- Distinct dates: 3,652
- Failed to parse: 0

## 8. Primary Key candidates

| Table | Column | Is unique? | n_distinct | n_rows | n_dups |
|-------|--------|-----------|------------|--------|--------|
| products | product_id | ✓ True | 2,412 | 2,412 | 0 |
| customers | customer_id | ✓ True | 121,930 | 121,930 | 0 |
| customers | zip | ⚠️ False | 31,491 | 121,930 | 90,439 |
| promotions | promo_id | ✓ True | 50 | 50 | 0 |
| geography | zip | ✓ True | 39,948 | 39,948 | 0 |
| orders | order_id | ✓ True | 646,945 | 646,945 | 0 |
| orders | customer_id | ⚠️ False | 90,246 | 646,945 | 556,699 |
| orders | zip | ⚠️ False | 29,932 | 646,945 | 617,013 |
| order_items | order_id | ⚠️ False | 646,945 | 714,669 | 67,724 |
| order_items | product_id | ⚠️ False | 1,598 | 714,669 | 713,071 |
| order_items | promo_id | ⚠️ False | 50 | 714,669 | 714,619 |
| payments | order_id | ✓ True | 646,945 | 646,945 | 0 |
| shipments | order_id | ✓ True | 566,067 | 566,067 | 0 |
| returns | return_id | ✓ True | 39,939 | 39,939 | 0 |
| returns | order_id | ⚠️ False | 36,062 | 39,939 | 3,877 |
| returns | product_id | ⚠️ False | 1,286 | 39,939 | 38,653 |
| reviews | review_id | ✓ True | 113,551 | 113,551 | 0 |
| reviews | order_id | ⚠️ False | 111,369 | 113,551 | 2,182 |
| reviews | product_id | ⚠️ False | 1,412 | 113,551 | 112,139 |
| reviews | customer_id | ⚠️ False | 48,676 | 113,551 | 64,875 |
| inventory | product_id | ⚠️ False | 1,624 | 60,247 | 58,623 |

## 9. Foreign Key Orphans

| Child.col | → Parent.col | n_orphans | pct_orphans |
|-----------|--------------|-----------|-------------|
| ✓ orders.customer_id | customers.customer_id | 0 | 0.00% |
| ✓ orders.zip | geography.zip | 0 | 0.00% |
| ✓ order_items.order_id | orders.order_id | 0 | 0.00% |
| ✓ order_items.product_id | products.product_id | 0 | 0.00% |
| ✓ payments.order_id | orders.order_id | 0 | 0.00% |
| ✓ shipments.order_id | orders.order_id | 0 | 0.00% |
| ✓ returns.order_id | orders.order_id | 0 | 0.00% |
| ✓ returns.product_id | products.product_id | 0 | 0.00% |
| ✓ reviews.order_id | orders.order_id | 0 | 0.00% |
| ✓ reviews.product_id | products.product_id | 0 | 0.00% |
| ✓ reviews.customer_id | customers.customer_id | 0 | 0.00% |
| ✓ inventory.product_id | products.product_id | 0 | 0.00% |
| ✓ customers.zip | geography.zip | 0 | 0.00% |

## 10. Cardinality between tables (actual from data)

| Left | Right | left_max_rows | right_max_rows | Inferred cardinality |
|------|-------|---------------|----------------|---------------------|
| orders.order_id | payments.order_id | 1 | 1 | **1:1** |
| orders.order_id | shipments.order_id | 1 | 1 | **1:1** |
| orders.order_id | order_items.order_id | 1 | 5 | **1:N** |
| orders.order_id | returns.order_id | 1 | 3 | **1:N** |
| orders.order_id | reviews.order_id | 1 | 3 | **1:N** |
| products.product_id | inventory.product_id | 1 | 126 | **1:N** |

## 11. Full duplicate rows

| Table | n_full_duplicates | pct |
|-------|-------------------|-----|
| ✓ products | 0 | 0.0000% |
| ✓ customers | 0 | 0.0000% |
| ✓ promotions | 0 | 0.0000% |
| ✓ geography | 0 | 0.0000% |
| ✓ orders | 0 | 0.0000% |
| ✓ order_items | 0 | 0.0000% |
| ✓ payments | 0 | 0.0000% |
| ✓ shipments | 0 | 0.0000% |
| ✓ returns | 0 | 0.0000% |
| ✓ reviews | 0 | 0.0000% |
| ✓ sales | 0 | 0.0000% |
| ✓ inventory | 0 | 0.0000% |
| ✓ web_traffic | 0 | 0.0000% |

## 12. Logical Sanity Checks

- products: cogs >= price: 0 rows
- products: price <= 0: 0, cogs < 0: 0
- shipments: ship_date > delivery_date: 0 rows
- promotions: start_date > end_date: 0 rows
- order_date < signup_date (đặt hàng trước khi đăng ký?): 477,453 rows
- returns: refund_amount < 0: 0, == 0: 0
- order_items: quantity <= 0: 0 rows
- order_items: unit_price <= 0: 0 rows
- order_items: discount_amount < 0: 0 rows

## 13. shipments ↔ order_status (distribution thực tế)

```
              no_shipment  has_shipment   Total
order_status                                   
cancelled           59462             0   59462
created              7275             0    7275
delivered             524        516192  516716
paid                13577             0   13577
returned               29         36113   36142
shipped                11         13762   13773
Total               80878        566067  646945
```

Đọc bảng: "status nào có shipment?" và "có status lạ nào không?"
Để team quyết định: status nào nên có shipment, status nào nên KHÔNG có.

### returns ↔ order_status

```
has_return         0      1   Total
order_status                       
cancelled      59462      0   59462
created         7275      0    7275
delivered     516716      0  516716
paid           13577      0   13577
returned          80  36062   36142
shipped        13773      0   13773
Total         610883  36062  646945
```
