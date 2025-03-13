CREATE TABLE air_quality_data (
    date VARCHAR(50),    
    unit VARCHAR(20),        
    indicator VARCHAR(50),  
    periodicity VARCHAR(20),   
    quarter VARCHAR(10),      
    CO DOUBLE PRECISION,      
    SO2 DOUBLE PRECISION,     
    NO2 DOUBLE PRECISION,     
    O3 DOUBLE PRECISION,      
    PM10 DOUBLE PRECISION     
);