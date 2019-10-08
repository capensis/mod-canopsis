#!/usr/bin/python

# -*- coding: utf-8 -*-

import sys
import os
import pickle
import socket
import traceback
import time

from socket import socket
from collections import deque

try:
    from kombu import BrokerConnection
    from kombu import Producer, Consumer, Exchange, Queue
except ImportError:
    BrokerConnection = None

from shinken.brokermodule import WorkerBasedBrokerModule
from shinken.brokermoduleworker import BrokerModuleWorker
from shinken.log import LoggerFactory
from shinken.message import Message

logger = LoggerFactory.get_logger('canopsis')

properties = {
    'daemons' : ['broker'],
    # JEAN: j'ai renomme le type de module car on va ranger les modules de cette maniere dans le futur
    'type'    : 'broker_exporter_canopsis',
    'external': False,
}


# Called by the plugin manager to get a broker
def get_instance(mod_conf):
    logger.set_display_name(mod_conf.get_name())
    logger.info("Get a canopsis data module for plugin %s" % mod_conf.get_name())
    instance = Canopsis_broker(mod_conf)
    return instance


# JEAN: c'est le worker qui va vraiment travailler, et avoir son propre processus
class CanopsisModuleWorker(BrokerModuleWorker):
    
    # JEAN: il n'y a plus de tick appele, a la place il y a un vrai main avec son propre thread.
    def worker_main(self):
        while True:
            logger.info('[worker:%s]Loop for canopsis worker process' % self._worker_id)
            if self.canopsis:
                self.canopsis.hook_tick()
            time.sleep(1)
    
    
    # JEAN: les init se font dans cette fonction
    def init_worker_before_main(self, module_configuration):
        self.host = getattr(module_configuration, 'host', None)
        self.port = getattr(module_configuration, 'port', None)
        self.user = getattr(module_configuration, 'user', None)
        self.password = getattr(module_configuration, 'password', None)
        self.virtual_host = getattr(module_configuration, 'virtual_host', None)
        self.exchange_name = getattr(module_configuration, 'exchange_name', None)
        self.identifier = getattr(module_configuration, 'identifier', None)
        self.maxqueuelength = getattr(module_configuration, 'maxqueuelength', 10000)
        self.queue_dump_frequency = getattr(module_configuration, 'queue_dump_frequency', 300)
        
        self.canopsis = event2amqp(self.host, self.port, self.user, self.password, self.virtual_host, self.exchange_name, self.identifier, self.maxqueuelength, self.queue_dump_frequency)
        
        self.last_need_data_send = time.time()
    
    
    def _ask_reinit(self, c_id):
        # Do not ask data too quickly, very dangerous
        # one a minute
        if time.time() - self.last_need_data_send > 60:
            logger.info('[Canopsis] Ask the broker for instance id (#{0}) data'.format(c_id))
            
            msg = Message(id=0, type='NeedData', data={'full_instance_id': c_id}, source=self.get_name())
            # JEAN: on a renomme le propriete pour renvoyer un message au broker
            self.from_module_to_main_daemon_queue.put(msg)
            
            self.last_need_data_send = time.time()
    
    
    def manage_initial_host_status_brok(self, b):
        logger.info("[Canopsis] processing initial_host_status")
        
        if not hasattr(self, 'host_commands'):
            self.host_commands = {}
        
        if not hasattr(self, 'host_addresses'):
            self.host_addresses = {}
        
        if not hasattr(self, 'host_max_check_attempts'):
            self.host_max_check_attempts = {}
        
        # check commands does not appear in check results so build a dict of check_commands
        self.host_commands[b.data['host_name']] = b.data['check_command'].call
        
        # address does not appear in check results so build a dict of addresses
        self.host_addresses[b.data['host_name']] = b.data['address']
        
        # max_check_attempts does not appear in check results so build a dict of max_check_attempts
        self.host_max_check_attempts[b.data['host_name']] = b.data['max_check_attempts']
        
        logger.debug("[canopsis] initial host max attempts: %s " % str(self.host_max_check_attempts))
        logger.debug("[canopsis] initial host commands: %s " % str(self.host_commands))
        logger.debug("[canopsis] initial host addresses: %s " % str(self.host_addresses))
    
    
    def manage_initial_service_status_brok(self, b):
        logger.debug("[Canopsis] processing initial_service_status")
        
        if not hasattr(self, 'service_commands'):
            logger.debug("[Canopsis] creating empty dict in service_commands")
            self.service_commands = {}
        
        if not hasattr(self, 'service_max_check_attempts'):
            logger.debug("[Canopsis] creating empty dict in service_max_check_attempts")
            self.service_max_check_attempts = {}
        
        if not b.data['host_name'] in self.service_commands:
            logger.debug("[Canopsis] creating empty dict for host %s service_commands" % b.data['host_name'])
            self.service_commands[b.data['host_name']] = {}
        
        self.service_commands[b.data['host_name']][b.data['service_description']] = b.data['check_command'].call
        
        if not b.data['host_name'] in self.service_max_check_attempts:
            logger.debug("[Canopsis] creating empty dict for host %s service_max_check_attempts" % b.data['host_name'])
            self.service_max_check_attempts[b.data['host_name']] = {}
        
        self.service_max_check_attempts[b.data['host_name']][b.data['service_description']] = b.data['max_check_attempts']
    
    
    def manage_host_check_result_brok(self, b):
        try:
            message = self.create_message('component', 'check', b)
        except Exception:
            logger.error("[Canopsis] Error: there was an error while trying to create message for host: %s" % traceback.format_exc())
            return
        
        if not message:
            logger.warning("[Canopsis] Warning: Empty host check message")
        else:
            self.push2canopsis(message)
    
    
    # A service check has just arrived. Write the performance data to the file
    def manage_service_check_result_brok(self, b):
        try:
            message = self.create_message('resource', 'check', b)
        except Exception:
            logger.error("[Canopsis] Error: there was an error while trying to create message for service: %s" % traceback.format_exc())
            return
        
        if not message:
            logger.warning("[Canopsis] Warning: Empty service check message")
        else:
            self.push2canopsis(message)
    
    
    def create_message(self, source_type, event_type, b):
        """
            event_type should be one of the following:
                - check
                - ack
                - notification
                - downtime

            source_type should be one of the following:
                - component => host
                - resource => service

            message format (check):

            H S         field               desc
            x           'connector'         Connector type (gelf, nagios, snmp, ...)
            x           'connector_name':   Connector name (nagios1, nagios2 ...)
            x           'event_type'        Event type (check, log, trap, ...)
            x           'source_type'       Source type (component or resource)
            x           'component'         Component name
            x           'resource'          Resource name
            x           'timestamp'         UNIX seconds timestamp
            x           'state'             State (0 (Ok), 1 (Warning), 2 (Critical), 3 (Unknown))
            x           'state_type'        State type (O (Soft), 1 (Hard))
            x           'output'            Event message
            x           'long_output'       Event long message
            x           'perfdata'          nagios plugin perfdata raw (for the moment)
            x           'check_type'
            x           'current_attempt'
            x           'max_attempts'
            x           'execution_time'
            x           'latency'
            x           'command_name'
                        'address'
        """
        
        if source_type == 'resource':
            commands = self.service_commands
        
        elif source_type == 'component':
            commands = self.host_commands
        
        else:
            logger.warning("[Canopsis] Invalid source_type %s" % (source_type))
            return None
        
        # Check if we need to re-init the broker
        reinit_needed = b.data['host_name'] not in commands
        reinit_needed = reinit_needed or (source_type == 'resource' and b.data['service_description'] not in commands[b.data['host_name']])
        
        if reinit_needed:
            self._ask_reinit(b.data['instance_id'])
        
        # Build the message
        
        if source_type == 'resource':
            # service
            specificmessage = {
                'resource'    : b.data['service_description'],
                'command_name': self.service_commands[b.data['host_name']][b.data['service_description']],
                'max_attempts': self.service_max_check_attempts[b.data['host_name']][b.data['service_description']],
            }
        elif source_type == 'component':
            # host
            specificmessage = {
                'resource'          : None,
                'command_name'      : self.host_commands[b.data['host_name']],
                'max_check_attempts': self.host_max_check_attempts[b.data['host_name']]
            }
        logger.info('MESSAGE: %s %s' % (b.type, b.data))
        commonmessage = {
            'connector'      : u'shinken',
            'connector_name' : unicode(self.identifier),
            'event_type'     : event_type,
            'source_type'    : source_type,
            'component'      : b.data['host_name'],
            'timestamp'      : b.data['last_chk'],
            'state'          : b.data['state_id'],
            'state_type'     : b.data['state_type_id'],
            'output'         : b.data['output'],
            'long_output'    : b.data['long_output'],
            'perf_data'      : b.data['perf_data'],
            'check_type'     : b.data['check_type'],
            'current_attempt': b.data['attempt'],
            # JEAN: je mets ces champs a 0 car on n'envoie plus cette info. Si tu en as besoin on peux voir comment la renvoyer et quand.
            'execution_time' : 0,
            'latency'        : 0,
            'address'        : self.host_addresses[b.data['host_name']]
        }
        
        return dict(commonmessage, **specificmessage)
    
    
    def push2canopsis(self, message):
        strmessage = str(message)
        self.canopsis.postmessage(message)


# Class for the Canopsis Broker
# Get broks and send them to a Carbon instance of Canopsis
class Canopsis_broker(WorkerBasedBrokerModule):
    MODULE_WORKER_CLASS = CanopsisModuleWorker


class event2amqp():
    
    def __init__(self, host, port, user, password, virtual_host, exchange_name, identifier, maxqueuelength, queue_dump_frequency):
        
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.virtual_host = virtual_host
        self.exchange_name = exchange_name
        self.identifier = identifier
        self.maxqueuelength = maxqueuelength
        self.queue_dump_frequency = queue_dump_frequency
        
        self.connection_string = None
        
        self.connection = None
        self.channel = None
        self.producer = None
        self.exchange = None
        self.queue = deque([])
        
        self.tickage = 0
        
        self.retentionfile_path = os.path.join(os.getcwd(), 'canopsis.dat')
        self.load_queue()
    
    
    def create_connection(self):
        self.connection_string = "amqp://%s:%s@%s:%s/%s" % (self.user, self.password, self.host, self.port, self.virtual_host)
        
        try:
            self.connection = BrokerConnection(self.connection_string)
            return True
        
        except:
            func = sys._getframe(1).f_code.co_name
            error = str(sys.exc_info()[0])
            logger.error("[Canopsis] Unexpected error: %s in %s" % (error, func))
            return False
    
    
    def connect(self):
        logger.info("[Canopsis] connection with: %s" % self.connection_string)
        try:
            self.connection.connect()
            if not self.connected():
                return False
            else:
                self.get_channel()
                self.get_exchange()
                self.create_producer()
                return True
        
        except:
            func = sys._getframe(1).f_code.co_name
            error = str(sys.exc_info()[0])
            logger.error("[Canopsis] Unexpected error: %s in %s" % (error, func))
            return False
    
    
    def disconnect(self):
        try:
            if self.connected():
                self.connection.release()
            return True
        except:
            func = sys._getframe(1).f_code.co_name
            error = str(sys.exc_info()[0])
            logger.error("[Canopsis] Unexpected error: %s in %s" % (error, func))
            return False
    
    
    def connected(self):
        if not self.connection:
            return False
        
        try:
            # Try to establish a connection
            if self.connection._connection:
                self.connection._closed = not self.connection.transport.verify_connection(self.connection._connection)
            
            else:
                return False
        
        except socket.error:
            # The socket is closed
            self.connection._closed = True
        
        return self.connection.connected
    
    
    def get_channel(self):
        try:
            self.channel = self.connection.channel()
        except:
            func = sys._getframe(1).f_code.co_name
            error = str(sys.exc_info()[0])
            logger.error("[Canopsis] Unexpected error: %s in %s" % (error, func))
            return False
    
    
    def get_exchange(self):
        try:
            self.exchange = Exchange(self.exchange_name, "topic", durable=True, auto_delete=False)
        except:
            func = sys._getframe(1).f_code.co_name
            error = str(sys.exc_info()[0])
            logger.error("[Canopsis] Unexpected error: %s in %s" % (error, func))
            return False
    
    
    def create_producer(self):
        try:
            self.producer = Producer(
                channel=self.channel,
                exchange=self.exchange,
                routing_key=self.virtual_host
            )
        except:
            func = sys._getframe(1).f_code.co_name
            error = str(sys.exc_info()[0])
            logger.error("[Canopsis] Unexpected error: %s in %s" % (error, func))
            return False
    
    
    def postmessage(self, message, retry=False):
        
        # process enqueud events if possible
        self.pop_events()
        
        key = "%s.%s.%s.%s.%s" % (
            message["connector"],
            message["connector_name"],
            message["event_type"],
            message["source_type"],
            message["component"]
        )
        
        if message["source_type"] == "resource":
            key = "%s.%s" % (
                key,
                message["resource"]
            )
        
        # connection management
        if not self.connected():
            logger.info("[Canopsis] Create connection")
            self.create_connection()
            self.connect()
        
        # publish message
        if self.connected():
            logger.debug("[Canopsis] using routing key %s" % key)
            logger.debug("[Canopsis] sending %s" % str(message))
            try:
                self.producer.revive(self.channel)
                self.producer.publish(body=message, compression=None, routing_key=key, exchange=self.exchange_name)
                return True
            except:
                self.connection.release()
                
                logger.error("[Canopsis] Impossible to publish message, adding it to queue (%s items in queue | max %s)" % (str(len(self.queue)), str(self.maxqueuelength)))
                
                if len(self.queue) < int(self.maxqueuelength):
                    self.queue.append({"key": key, "message": message})
                
                func = sys._getframe(1).f_code.co_name
                error = str(sys.exc_info()[0])
                logger.error("[Canopsis] Unexpected error: %s in %s" % (str(error), func))
                
                return False
        else:
            self.connection.release()
            
            errmsg = "[Canopsis] Not connected, going to queue messages until connection back (%s items in queue | max %s)" % (str(len(self.queue)), str(self.maxqueuelength))
            logger.error(errmsg)
            
            if len(self.queue) < int(self.maxqueuelength):
                self.queue.append({"key": key, "message": message})
                logger.debug("[Canopsis] Queue length: %d" % len(self.queue))
                return True
            
            else:
                logger.error("[Canopsis] Maximum retention for event queue %s reached" % str(self.maxqueuelength))
                return False
    
    
    def errback(self, exc, interval):
        logger.warning("Couldn't publish message: %r. Retry in %ds" % (exc, interval))
    
    
    def pop_events(self):
        if self.connected():
            while len(self.queue) > 0:
                item = self.queue.pop()
                try:
                    logger.debug("[Canopsis] Pop item from queue [%s]: %s" % (str(len(self.queue)), str(item)))
                    self.producer.revive(self.channel)
                    self.producer.publish(body=item["message"], compression=None, routing_key=item["key"], exchange=self.exchange_name)
                except:
                    self.queue.append(item)
                    func = sys._getframe(1).f_code.co_name
                    error = str(sys.exc_info()[0])
                    logger.error("[Canopsis] Unexpected error: %s in %s" % (error, func))
                    return False
        else:
            return False
    
    
    def hook_tick(self):
        self.tickage += 1
        
        # queue retention saving
        if self.tickage >= int(self.queue_dump_frequency) and len(self.queue) > 0:
            # flush queue to disk if queue age reach queue_dump_frequency
            self.save_queue()
            self.tickage = 0
        
        return True
    
    
    def save_queue(self):
        if not os.path.exists(self.retentionfile_path):
            open(self.retentionfile_path, 'w').close()
        
        logger.info("[Canopsis] saving to %s" % self.retentionfile_path)
        filehandler = open(self.retentionfile_path, 'w')
        pickle.dump(self.queue, filehandler)
        filehandler.close()
        
        return True
    
    
    def load_queue(self):
        if not os.path.exists(self.retentionfile_path):
            open(self.retentionfile_path, 'w').close()
        
        logger.info("[Canopsis] loading from %s" % self.retentionfile_path)
        filehandler = open(self.retentionfile_path, 'r')
        
        try:
            self.queue = pickle.load(filehandler)
        except:
            pass
        
        return True
