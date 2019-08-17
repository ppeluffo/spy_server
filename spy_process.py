#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 19:55:43 2019

@author: pablo
"""

import os
import configparser
import sys
import glob
import time
import re
import spy_log as log

# -----------------------------------------------------------------------------
Config = configparser.ConfigParser()
Config.read('spy.conf')
#
from spy_bd_general import BD
bd = BD()

# ------------------------------------------------------------------------------

def format_fecha_hora(fecha, hora):
    '''
    Funcion auxiliar que toma la fecha y hora de los argumentos y
	las retorna en formato DATE de la BD.
    121122,180054 ==> 2012-11-22 18:00:54
    '''
    lfecha = [(fecha[i:i + 2]) for i in range(0, len(fecha), 2)]
    lhora = [(hora[i:i + 2]) for i in range(0, len(hora), 2)]
    timestamp = '%s%s-%s-%s %s:%s:%s' % ( lfecha[0], lfecha[1], lfecha[2], lfecha[3], lhora[0], lhora[1], lhora[2] )
    return(timestamp)


def process_line(dlgid, line):
    '''
    Recibo una linea, la parseo y dejo los campos en un diccionario
    Paso este diccionario a la BD para que la inserte.
    '''
    #LOG.info('process_: line: [%s]' % line)
    line = line.rstrip('\n|\r|\t')
    fields = re.split(',', line )
    d = dict()
    d['timestamp'] = format_fecha_hora(fields[0], fields[1])
    for field in fields[2:]:
        key, value = re.split('=', field)
        d[key] = value
    #LOG.info('process_: dict=%s' % d)
    bd.insert_data_line(dlgid, d)
    return


def process_file(file):
    '''
    Recibo el nombre de un archivo el cual abro y voy leyendo c/linea
    y procesandola
    Al final lo muevo al directorio de backups
    c/archivo puede corresponder a un datalogger distinto por lo tanto el datasource puede ser
    distinto.
    Debo entonces resetear el datasource antes de procesar c/archivo
    '''
    dirname, filename = os.path.split(file)
    LOG.info('process_: file: %s' % filename)
    dlgid, *res = re.split('_', filename)
    #LOG.info('process_: dlgid: %s' % dlgid)
    bd.reset_datasource(dlgid)
    with open(file) as myfile:
        line = myfile.readline()
        process_line(dlgid, line)

    # Muevo el archivo al backup.
    try:
        bkdirname = Config['PROCESS']['process_bk_path']
        bkfile = os.path.join(bkdirname, filename)
        #LOG.info('process_: bkfile: %s' % bkfile)
        os.rename(file, bkfile)
    except:
        LOG.info('process_: [%s] no puedo pasar a bk' % file)
    return


if __name__ == '__main__':

    # Lo primero es configurar el logger
    LOG = log.config_logger()
    dirname = Config['PROCESS']['process_rx_path']
    LOG.info('process_: dirname: %s' % dirname)

    while True:
        for file in glob.glob(dirname + '/*.dat'):
            process_file(file)
        time.sleep(60)

