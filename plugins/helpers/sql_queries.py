class SqlQueries:
    state_table_insert = ("""
            INSERT INTO state (
                SELECT DISTINCT ds.state_code, ds.state
                FROM i94staging i94 INNER JOIN demo_staging ds 
                ON i94.i94addr = ds.state_code);
        """)

    city_table_insert = ("""
            INSERT INTO city (
                SELECT DISTINCT ds.city, ds.state, ds.median_age, ds.male_pop, ds.female_pop, 
                ds.total_pop, ds.no_vet, ds.foreign_born, ds.avg_household_size, ds.state_code, ds.race, ds.count
                FROM i94staging i94 INNER JOIN demo_staging ds 
                ON i94.i94addr = ds.state_code);
        """)
    country_table_insert = ("""
            INSERT INTO country (
                SELECT DISTINCT c.code, c.country
                FROM i94staging i94 INNER JOIN country_lookup c
                ON i94.i94res = c.code);
        """)

    visa_table_insert = ("""
            INSERT INTO visa (
                SELECT DISTINCT v.visa_code, v.visa_type
                FROM i94staging i94 INNER JOIN visa_lookup v
                ON i94.i94visa = v.visa_code);
        """)

    port_table_insert = ("""
            INSERT INTO port (
                SELECT DISTINCT p.port_code, p.port_name
                FROM i94staging i94 INNER JOIN port_lookup p 
                ON i94.i94port = p.port_code);
        """)
    i94fact_table_insert = ("""
            INSERT INTO i94fact (
                SELECT DISTINCT i.ccid, i.i94res, i.i94addr, i.i94port, i.i94bir, i.i94visa, i.biryear, i.gender
                FROM i94staging i 
                    JOIN state s ON i.i94addr = s.state_code
                    JOIN country c ON i.i94res = c.code
                    JOIN port ON i.i94port = port.port_code
                    JOIN visa ON i.i94visa = visa.visa_code   
                );
        """)
