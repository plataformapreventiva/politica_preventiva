#!/usr/bin/env python

import click
import socket
import sys
import luigi
import politica_preventiva.pipelines.politica_preventiva


def check_server(host, port):
    """
    Checks if Luigi Server is running to use the central scheduler
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server.connect((host, port))
        return True
    except socket.error as e:
        return False
    finally:
        server.close()

@click.command()
@click.option('--server', help='URL donde se encuentra el luigi-master', default='localhost')
@click.option('--port', help='Puerto donde está escuchando el luigi-master', default=8082)
@click.option('--luigi_cfg', help='Path al archivo de configuración de Luigi', type=click.Path())
@click.option('--workers', help='Número de workers en paralelo', type=click.INT, default=1)
@click.option('--level', help='', default='ETLPipeline')
@click.option('--server', help='', default='0.0.0.0')
@click.option('--ptask', help='', default='auto')
def main(server, port, luigi_cfg, workers, level, ptask):
    """
    Ejecuta el pipeline
    """

    luigi_args = [

                  '--scheduler-host', str(server),
                  '--scheduler-port', str(port),
                  'RunPipelines',
                  # '--sleep', str(sleep),
                  '--workers', str(workers),
                  '--no-lock',
                  '--level', str(level),
                  '--ptask', str(ptask)]

    # If the server is not running then it uses the local scheduler
    if not check_server(server, port):
        luigi_args.append('--local-scheduler')

    luigi.run(luigi_args)


if __name__ == '__main__':
    main()
