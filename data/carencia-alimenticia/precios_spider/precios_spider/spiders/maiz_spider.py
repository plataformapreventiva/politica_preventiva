import scrapy
from precios_spider.items import PreciosSpiderItem
import re

class MaizSpider(scrapy.Spider):
    """
Spider para obtener precios semanales de maiz en distintas centrales de abasto de México. 
Itera sobre cinco posibles semanas en un mes. 

En caso de no tener 
    """

    name = "precios_granos"

    def __init__(self, producto='Maiz blanco'): 
        self.producto = producto 

    def start_requests(self):
        producto_dict = {
         'Alubia chica': '598',
         'Alubia grande': '599',
         'Arroz pulido Morelos': '1',
         'Arroz pulido Sinaloa': '2',
         'Arroz pulido sin especificar': '3',
         'Arroz pulido tipo Morelos': '4',
         'Frijol Azufrado': '332',
         'Frijol Bayo': '334',
         'Frijol Bayo berrendo': '333',
         'Frijol Cacahuate bola': '335',
         'Frijol Cacahuate largo': '336',
         'Frijol Canario': '337',
         'Frijol Colorado': '338',
         'Frijol Flor de junio': '339',
         'Frijol Flor de mayo': '340',
         'Frijol Garbancillo': '341',
         'Frijol Garbancillo zarco': '342',
         'Frijol Mayocoba': '343',
         'Frijol Negro': '347',
         'Frijol Negro Nayarit': '346',
         'Frijol Negro Veracruz': '348',
         'Frijol Negro bola': '344',
         'Frijol Negro importado': '345',
         'Frijol Ojo de cabra': '349',
         'Frijol Peruano': '350',
         'Frijol Pinto': '352',
         'Frijol Pinto importado': '351',
         'Frijol Serahui': '353',
         'Frijol Tepari': '354',
         'Frijol Yurimun': '355',
         'Garbanzo chico': '600',
         'Garbanzo grande': '601',
         'Haba': '602',
         'Lenteja chica': '603',
         'Lenteja grande': '604',
         'Maiz blanco': '605',
         'Maiz blanco pozolero': '606'}

        months = list(range(12,0,-1))
        weeks =list(range(5,0,-1))
        years = list(range(2016, 2000, -1))
        base_url = "http://www.economia-sniim.gob.mx/nuevo/Consultas/MercadosNacionales/PreciosDeMercado/Agricolas/ResultadosConsultaFechaGranos.aspx?Semana={0}&Mes={1}&Anio={2}&ProductoId={3}&OrigenId=-1&Origen=Todos&DestinoId=-1&Destino=Todos&RegistrosPorPagina=%20500"
        urls = [base_url.format(week, month, year, producto_dict[self.producto]) for week in weeks for month in months for year in years]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        sel = response.selector
        rows = sel.xpath('//table[@id="tblResultados"]/child::*')
        if rows:
            for row in rows:
                if row.xpath('./td[1]/text()').extract_first().lower() != 'fecha':
                    item = PreciosSpiderItem()
                    item['fecha'] = row.xpath('./td[1]/text()').extract_first()
                    try:
                        origen = row.xpath('./td[2]/text()').extract_first().lower()
                    except Exception:
                        origen = ''
                    item['origen'] = origen
                    destino = row.xpath('./td[3]/text()').extract_first()
                    item['edo_destino'] = self.parse_destino(destino, 'estado')
                    item['destino'] = self.parse_destino(destino, 'completo')
                    item['precio_min'] = row.xpath('./td[4]/text()').extract_first()
                    item['precio_max'] = row.xpath('./td[5]/text()').extract_first()
                    item['precio_frec'] = row.xpath('./td[6]/text()').extract_first()
                    item['obs'] = row.xpath('./td[7]/text()').extract_first()
                    item['producto'] = self.producto
                    yield item


    def parse_destino(self, destino, tipo):
        try:
            destino = destino.lower()
            a = re.split('\:', destino)
            if len(a) == 2:
                if tipo == 'estado':
                    return a[0]
                else:
                    return a[1]
            else:
                return ""
        except Exception as e:
            return ""

