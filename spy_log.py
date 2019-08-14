#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 09:19:01 2019

@author: pablo
"""

import logging
import logging.handlers


def config_logger():
    # logging.basicConfig(filename='log1.log', filemode='a', format='%(asctime)s %(name)s %(levelname)s %(message)s', level = logging.DEBUG, datefmt = '%d/%m/%Y %H:%M:%S' )
    logging.basicConfig(level=logging.DEBUG)

    # formatter = logging.Formatter('SPX %(asctime)s  [%(levelname)s] [%(name)s] %(message)s', datefmt='%Y-%m-%d %T')
    formatter = logging.Formatter('SPX [%(levelname)s] [%(name)s] %(message)s')
    handler = logging.handlers.SysLogHandler('/dev/log')
    handler.setFormatter(formatter)
    # Creo un logger derivado del root para usarlo en los modulos
    logger1 = logging.getLogger()
    # Le leo el handler de consola para deshabilitarlo
    lhStdout = logger1.handlers[0]  # stdout is the only handler initially
    logger1.removeHandler(lhStdout)
    # y le agrego el handler del syslog.
    logger1.addHandler(handler)
    # Creo ahora un logger child local.
    LOG = logging.getLogger('spy')
    LOG.addHandler(handler)
    return LOG

# ------------------------------------------------------------------------------
