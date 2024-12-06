# SATMET HOURLY


## **Descrição**
Este projeto tem como objetivo baixar dados SAMeT (South American Mapping of Temperature) do INPE CPTEC em formato **NetCDF** de um servidor FTP, a cada hora, converter esses dados para **GeoTIFF**, calcular **estatísticas zonais** para um grid espacial definido e armazenar os resultados em uma tabela do **PostgreSQL**. Ele utiliza variáveis de ambiente armazenadas em um arquivo **`.env`** para carregar configurações sensíveis.



