Predicate Shift. （query center distribution）
这里我们就用两个不同的分布来展示把
[DONE]


Frequency Modulation
这个给出四个时间段 时间段中查询分布有四种。
[DONE]


Selectivity Shift.
这里我们给出一个selectivity从 a 到 b 变化的过程中 range的变化 然后带上时间。
[DONE]


Template Mutation
这个用t-SNE 表示
[DONE]



这个还没有测试到
    "sample_rich.category": {
        "distribution": "choice",
        "choices": ["A", "B", "C"]
    },
    "sample_rich.signup_date": {
        "distribution": "fixed",
        "value": "2021-01-01"
    }

任何类型如果种类小于某些数量就可以 认定为categorical


cs_warehouse_sk 的范围不对，可能需要指定，也需要来好好的确定范围。。

pgdb=# SELECT public.catalog_sales.cs_catalog_page_sk, public.catalog_sales.cs_ext_tax FROM public.catalog_sales FULL JOIN public.store_sales ON public.catalog_sales.cs_ext_tax = public.store_sales.ss_ext_tax WHERE public.catalog_sales.cs_warehouse_sk = 1 limit 10;
 cs_catalog_page_sk | cs_ext_tax
--------------------+------------
                 75 |      13.82
                 75 |      13.82
                 75 |      13.82
                 75 |      13.82
                 75 |      13.82
                 75 |      13.82
                 75 |      13.82
                 75 |      13.82
                 75 |      13.82
                 75 |      13.82
(10 rows)


LIMIT 和 aggregation 可以配置

一个数据库表，我想知道其中数据 然后随机生成一些。 
但是我怎么保证在生成的时候 如果表跟表直接有依赖生成 数据如何维持这些依赖呢？

这里有细节：https://chatgpt.com/c/6864d9ce-b2ec-8008-98cc-9dcedcf797de
