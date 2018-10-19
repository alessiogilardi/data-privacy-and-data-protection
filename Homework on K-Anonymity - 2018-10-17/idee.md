# Idee
- creo una copia del db su cui scrivere
- eseguo una query del tipo :
    "SELECT age, city_birth, zip_code, COUNT(*) as freq
    FROM '10000'
    GROUP BY age, city_birth, zip_code
    ORDER BY freq"
- controllo qual è la frequenza di ogni riga, se trovo una riga con frequenza minore di k allora:
    - eseguo: 
    SELECT COUNT(*) as zip_codes_num
    FROM (
        SELECT zip_code, COUNT(*) AS freq
        FROM '10000'
        GROUP BY zip_code)

    SELECT COUNT(*) as city_birth_num
    FROM (
        SELECT city_birth, COUNT(*) AS freq
        FROM '10000'
        GROUP BY city_birth)

    SELECT COUNT(*) as age_num
    FROM (
        SELECT age, COUNT(*) AS freq
        FROM '10000'
        GROUP BY age)
    - controllo qual è il numero maggiore
    - l'attributo con numero maggiore di righe è quello da generalizzare
    - lo generalizzo e ripeto sul set di dati generalizzato


UPDATE '10000_anon_prova'
SET city_birth = (
SELECT city_gen1
FROM 'city_birth_gen'
WHERE city_birth = city_gen0)

WHERE city_birth IN (
SELECT city_gen0
FROM 'city_birth_gen')









