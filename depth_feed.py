

"""
Connections to IQFeed.exe to get different types of data.

This module contains various Conn classes called XXXConn each of
which connects to IQFeed and helps to return market data from it.

Some of the XXXConn classes (like HistoryConn), which provide data
that should be available when requested, provide the data
requested as the return value of the function that requests the
data. Other XXXConn classes (like QuoteConn) which provide streaming
data, require you to implement a class that derives from one of the
Listeners in listeners.py and provide the data by calling lookbacks
in those classes as it comes in.

All XXXConn classes send status messages to listener classes. While
a listener class is not strictly necessary when using something like
HistoryConn, if things aren't working right, you may want to use a
listener to make sure you aren't getting a message telling you why
that you are ignoring.

Data that you are likely to use for analysis is returned as numpy
structured arrays. Other data is normally returned as a namedtuple
specific to that message time.

FeedConn is the base class for all the XXXConn classes.

QuoteConn provides real-time tick-data and real-time news headlines.

AdminConn provides status messages about the status of the Feed etc.

HistoryConn provides historical data.

LookupConn lets you lookup symbols and option and futures chains.

TableConn provides reference data like condition codes and exchanges.

BarConn lets you request real-time interval bars instead of calculating them
yourself from the tick-data (from QuoteConn).

NewsConn lets you get news-headlines in bulk (as opposed to real-time news
which you can get from QuoteConn) and full news stories from the story id.

See http://www.iqfeed.net/dev/main.cfm for more information.
"""



'''
Note! most of the code of this module come from https://github.com/akapur/pyiqfeed 
Some modifications had to be made in order to download the live depth data. 
'''



import os
import datetime
#import itertools
import select
import socket
import threading
import time as t 

from collections import deque, namedtuple
from typing import Sequence, List
import xml.etree.ElementTree as ElementTree

import numpy as np
from pyiqfeed.exceptions import NoDataError, UnexpectedField, UnexpectedMessage
from pyiqfeed.exceptions import UnexpectedProtocol, UnauthorizedError
from pyiqfeed import field_readers as fr


from localconfig.passwords import dtn_product_id, dtn_login, dtn_password
import pyiqfeed as iq
from pyiqfeed import VerboseIQFeedListener
import test_service

import depth_watchlist
import depth_data

class FeedConn:
    """
    FeedConn is the base class for other XXXConn classes

    It handles connecting, disconnecting, sending messages to IQFeed,
    reading responses from IQFeed, feed status messages etc.

    """

    protocol = "6.2"

    iqfeed_host = os.getenv('IQFEED_HOST', "127.0.0.1")
    quote_port = int(os.getenv('IQFEED_PORT_QUOTE', 5009))
    lookup_port = int(os.getenv('IQFEED_PORT_LOOKUP', 9100))
    depth_port = int(os.getenv('IQFEED_PORT_DEPTH', 9200))
    admin_port = int(os.getenv('IQFEED_PORT_ADMIN', 9300))
    deriv_port = int(os.getenv('IQFEED_PORT_DERIV', 9400))

    host = iqfeed_host
    port = quote_port

    databuf = namedtuple(
        "databuf", ('failed', 'err_msg', 'num_pts', 'raw_data'))

    def __init__(self, name: str, host: str, port: int):
        self._host = host
        self._port = port
        self._name = name

        self._stop = threading.Event()
        self._start_lock = threading.Lock()
        self._connected = False
        self._reconnect_failed = False
        self._pf_dict = {}
        self._sm_dict = {}
        self._listeners = []
        self._buf_lock = threading.RLock()
        self._send_lock = threading.RLock()
        self._recv_buf = ""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._read_thread = threading.Thread(group=None, target=self,
                                             name="%s-reader" % self._name,
                                             args=(), kwargs={}, daemon=None)
        self._set_message_mappings()

    
    def connect(self) -> None:
        """
        Connect to the appropriate socket and start the reading thread.

        You must call this before you start using an XXXConn class. If
        this thread is not running, no callbacks will be called, no data
        will be returned by functions which return data immediately.
        """
        
        self._sock.connect((self._host, self._port))
        self._set_protocol(FeedConn.protocol)
        self._set_client_name(self.name())
        self._send_connect_message()
        self.start_runner()


    def start_runner(self) -> None:
        """Called to start the reading thread."""
        with self._start_lock:
            self._stop.clear()
            if not self.reader_running():
                self._read_thread.start()
    

    def disconnect(self) -> None:
        """
        Stop the reading thread and disconnect from the socket to IQFeed.exe

        Call this to ensure sockets are closed and we exit cleanly.

        """
        self.stop_runner()
        if self._sock:
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
            self._sock = None

    def stop_runner(self) -> None:
        """Called to stop the reading and message processing thread."""
        with self._start_lock:
            self._stop.set()
            if self.reader_running():
                self._read_thread.join(30)

    def reader_running(self) -> bool:
        """
        True if the reader thread is running.

        If you don't get updates for a while you "may" want to query this
        function.  Mainly useful for debugging during development of the
        library.  If the reader thread is crashing, there is likely a bug
        in the library or something else is going very wrong.
        """
        return self._read_thread.is_alive()

    def connected(self) -> bool:
        """
        Returns true if IQClient.exe is connected to DTN's servers.

        It may take a few seconds after connecting to IQFeed for IQFeed to tell
        us it is connected to DTN's servers. During these few seconds, this
        function will return False even though it's not actually a problem.

        NOTE: It's not telling you if you are connected to IQFeed.exe. It's
        telling you if IQFeed.exe is connected to DTN's servers.
        """
        return self._connected

    def name(self) -> str:
        """Return whatever you named this conn class in the constructor"""
        return self._name

    def _send_cmd(self, cmd: str) -> None:
        with self._send_lock:
            self._sock.sendall(cmd.encode(encoding='latin-1'))

    def reconnect_failed(self) -> bool:
        """
        Returns true if IQClient.exe failed to reconnect to DTN's servers.

        It can and does happen that IQClient.exe drops a connection to DTN's
        servers and then reconnects. This is not a big problem. But if a
        reconnect fails this means there is a big problem and you should
        probably pause trading and figure out what's going on with your
        network.
        """
        return self._reconnect_failed

    def __call__(self):
        """The reader thread runs this in a loop."""
        while not self._stop.is_set():
            if self._read_messages():
                self._process_messages()

    def _read_messages(self) -> bool:
        """Read raw text sent by IQFeed on socket"""
        ready_list = select.select([self._sock], [], [self._sock], 5)
        if ready_list[2]:
            raise RuntimeError(
                    "Error condition on socket connection to IQFeed: %s,"
                    "" % self.name())
        if ready_list[0]:
            data_recvd = self._sock.recv(1024).decode('latin-1')
            with self._buf_lock:
                self._recv_buf += data_recvd
                return True
        return False

    def _next_message(self) -> str:
        """Next complete message from buffer of delimited messages"""
        with self._buf_lock:
            next_delim = self._recv_buf.find('\n')
            if next_delim != -1:
                message = self._recv_buf[:next_delim].strip()
                self._recv_buf = self._recv_buf[(next_delim + 1):]
                return message
            else:
                return ""

    def _set_message_mappings(self) -> None:
        """Creates map of message names to processing functions."""
        self._pf_dict['E'] = self._process_error
        self._pf_dict['T'] = self._process_timestamp
        self._pf_dict['S'] = self._process_system_message

        self._sm_dict["SERVER DISCONNECTED"] = \
            self._process_server_disconnected
        self._sm_dict["SERVER CONNECTED"] = self._process_server_connected
        self._sm_dict[
            "SERVER RECONNECT FAILED"] = self._process_reconnect_failed
        self._sm_dict["CURRENT PROTOCOL"] = self._process_current_protocol
        self._sm_dict["STATS"] = self._process_conn_stats


    def _process_messages(self) -> None:
        """Process the next complete message waiting to be processed"""
        message = self._next_message()
        
        
        while "" != message:
            fields = message.split(',')
            handle_func = self._processing_function(fields)
            handle_func(fields)
            message = self._next_message()

    def _processing_function(self, fields):
        """Returns the processing function for this specific message."""
        pf = self._pf_dict.get(fields[0][0])
        if pf is not None:
            return pf
        else:
            return self._process_unregistered_message

    def _process_unregistered_message(self, fields: Sequence[str]) -> None:
        """Called if we get a message we don't expect.

        Appropriate action here is probably to crash.

        """
        err_msg = ("Unexpected message received by %s: %s" % (
            self.name(), ",".join(fields)))
        
        print('printing fields:')
        print(fields)
        
        raise UnexpectedMessage(err_msg)

    def _process_system_message(self, fields: Sequence[str]) -> None:
        """
        Called when the next message is a system message.

        System messages are messages about the state of the data delivery
        system, including IQConnect.exe, DTN servers and connectivity.

        """
        assert len(fields) > 1
        assert fields[0] == "S"
        processing_func = self._system_processing_function(fields)
        
        processing_func(fields)

    def _system_processing_function(self, fields):
        """Returns the appropriate system message handling function."""
        assert len(fields) > 1
        assert fields[0] == "S"
        spf = self._sm_dict.get(fields[1])
        if spf is not None:
            return spf
        else:
            return self._process_unregistered_system_message

    def _process_unregistered_system_message(self,
                                             fields: Sequence[str]) -> None:
        """
        Called if we get a system message we don't know how to handle.

        Appropriate action here is probably to crash.

        """
        err_msg = ("Unexpected message received by %s: %s" % (
            self.name(), ",".join(fields)))
        raise UnexpectedMessage(err_msg)

    def _process_current_protocol(self, fields: Sequence[str]) -> None:
        """
        Process the Current Protocol Message

        The first message we send IQFeed.exe upon connecting is the
        set protocol message. If we get this message and the protocol
        IQFeed tells us it's using does not match the expected protocol
        then the we really need to shutdown, fix the version mismatch by
        upgrading/downgrading IQFeed.exe and this library so they match.

        """
        assert len(fields) > 2
        assert fields[0] == "S"
        assert fields[1] == "CURRENT PROTOCOL"
        protocol = fields[2]
        if protocol != FeedConn.protocol:
            err_msg = ("Desired Protocol %s, Server Says Protocol %s in %s" % (
                FeedConn.protocol, protocol, self.name()))
            raise UnexpectedProtocol(err_msg)

    def _process_server_disconnected(self, fields: Sequence[str]) -> None:
        """Called when IQFeed.exe disconnects from DTN's servers."""
        assert len(fields) > 1
        assert fields[0] == "S"
        assert fields[1] == "SERVER DISCONNECTED"
        self._connected = False
        for listener in self._listeners:
            listener.feed_is_stale()

    def _process_server_connected(self, fields: Sequence[str]) -> None:
        """Called when IQFeed.exe connects or re-connects to DTN's servers."""
        assert len(fields) > 1
        assert fields[0] == "S"
        assert fields[1] == "SERVER CONNECTED"
        self._connected = True
        for listener in self._listeners:
            listener.feed_is_fresh()

    def _process_reconnect_failed(self, fields: Sequence[str]) -> None:
        """Called if IQFeed.exe cannot reconnect to DTN's servers."""
        assert len(fields) > 1
        assert fields[0] == "S"
        assert fields[1] == "SERVER RECONNECT FAILED"
        self._reconnect_failed = True
        self._connected = False
        for listener in self._listeners:
            listener.feed_is_stale()
            listener.feed_has_error()

    ConnStatsMsg = namedtuple(
        'ConnStatsMsg', (
            'server_ip', 'server_port', 'max_sym', 'num_sym', 'num_clients',
            'secs_since_update', 'num_recon', 'num_fail_recon',
            'conn_tm', 'mkt_tm',
            'status', 'feed_version', 'login',
            'kbs_recv', 'kbps_recv', 'avg_kbps_recv',
            'kbs_sent', 'kbps_sent', 'avg_kbps_sent'))

    def _process_conn_stats(self, fields: Sequence[str]) -> None:
        """Parse and send ConnStatsMsg to listener."""
        assert len(fields) > 20
        assert fields[0] == "S"
        assert fields[1] == "STATS"

        # noinspection PyCallByClass
        conn_stats = FeedConn.ConnStatsMsg(
            server_ip=fields[2],
            server_port=fr.read_int(fields[3]),
            max_sym=fr.read_int(fields[4]),
            num_sym=fr.read_int(fields[5]),
            num_clients=fr.read_int(fields[6]),
            secs_since_update=fr.read_int(fields[7]),
            num_recon=fr.read_int(fields[8]),
            num_fail_recon=fr.read_int(fields[9]),
            conn_tm=(t.strptime(fields[10], "%b %d %I:%M%p")
                     if fields[10] != "" else None),
            mkt_tm=(t.strptime(fields[11], "%b %d %I:%M%p")
                    if self.connected() else None),
            status=(fields[12] == "Connected"),
            feed_version=fields[13],
            login=fields[14],
            kbs_recv=fr.read_float(fields[15]),
            kbps_recv=fr.read_float(fields[16]),
            avg_kbps_recv=fr.read_float(fields[17]),
            kbs_sent=fr.read_float(fields[18]),
            kbps_sent=fr.read_float(fields[19]),
            avg_kbps_sent=fr.read_float(fields[20]))
        for listener in self._listeners:
            listener.process_conn_stats(conn_stats)

    TimeStampMsg = namedtuple("TimeStampMsg", ("date", "time"))

    def _process_timestamp(self, fields: Sequence[str]) -> None:
        """Parse timestamp and send to listener."""
        # T,[YYYYMMDD HH:MM:SS]
        assert fields[0] == "T"
        assert len(fields) > 1
        dt_tm_tuple = fr.read_timestamp_msg(fields[1])
        # noinspection PyCallByClass
        timestamp = FeedConn.TimeStampMsg(date=dt_tm_tuple[0],
                                          time=dt_tm_tuple[1])
        for listener in self._listeners:
            listener.process_timestamp(timestamp)

    def _process_error(self, fields: Sequence[str]) -> None:
        """Called when IQFeed.exe sends us an error message."""
        assert fields[0] == "E"
        assert len(fields) > 1
        for listener in self._listeners:
            listener.process_error(fields)

    def add_listener(self, listener) -> None:
        """
        Call this to receive updates from this Conn class.

        :param listener: An object of the appropriate listener class.

        You need to call this function with each object that you want messages
        sent to. The object must be of (or derived from) the "appropriate"
        listener class. The various processing functions call callbacks in
        the listeners that have been registered to them when the Conn class
        receives messages.

        """
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_listener(self, listener) -> None:
        """
        Call this to unsubscribe the listener object.

        :param listener: An object of the appropriate listener class.

        You must call this if a listener class that has been subscribed for
        updates is going to be deleted. Since python is GC'd, if you don't
        do this the object won't actually be destroyed since the Conn class
        holds a handle to it and it will keep sending the object messages.

        You may want to add something that unsubscribes to the listener
        object's destructor.

        """
        if listener in self._listeners:
            self._listeners.remove(listener)

    def _set_protocol(self, protocol) -> None:
        self._send_cmd("S,SET PROTOCOL,%s\r\n" % protocol)

    def _send_connect_message(self) -> None:
        msg = "S,CONNECT\r\n"
        self._send_cmd(msg)

    def _send_disconnect_message(self) -> None:
        self._send_cmd("S,DISCONNECT\r\n")

    def _set_client_name(self, name) -> None:
        self._name = name
        msg = "S,SET CLIENT NAME,%s\r\n" % name
        self._send_cmd(msg)
        
        
        
        
        
        
        
  
   

class DepthConn(FeedConn):
    
    """
    DepthConn provides real-time Level 2 data.

    It derives from FeedConn
    so it also provides timestamps and feed status messages and
    everything else FeedConn provides.

    The way to use this class is to create the class, then create an object
    of a class written by your derived from QuoteListener and subscribe to
    updates from this class by calling the member function add_listener
    with the Listener object. Then call the members of this class to
    subscribe to updates.
    """
    
    host = FeedConn.host
    port = FeedConn.depth_port



    def __init__(self, name: str = "DepthConn", host: str = FeedConn.host,
                 port: int = port):
        super().__init__(name, host, port)
        self._current_update_fields = []
        self._update_names = []
        self._update_dtype = []
        self._update_reader = []
        self._set_message_mappings()
        
        self.dwl = depth_watchlist.depth_watchlist(init=True,active_liquid=True)
        self.watchlist = self.dwl.get_active_watchlist()
     

    
    def start_watchlist(self):
        for ticker in self.watchlist:
            self.request_market_depth(ticker)
    
    
    def update_watchlist(self):
        new_watchlist = self.dwl.get_active_watchlist()
        old_watchlist = self.watchlist
        
        self.watchlist = new_watchlist
        #print(self.watchlist)
        
        removed_ticker = []
        added_ticker = []
        
        for ticker in new_watchlist:
            if ticker not in old_watchlist:
                added_ticker.append(ticker)
                
        for ticker in old_watchlist:
            if ticker not in new_watchlist:
                removed_ticker.append(ticker)
                
        self.remove_ticker_list(removed_ticker)
        self.add_ticker_list(added_ticker)
                
    
    def remove_ticker_list(self,ticker_list):
        for ticker in ticker_list:
            self.remove_market_depth(ticker)
            
    
    def add_ticker_list(self,ticker_list):
        for ticker in ticker_list:
            self.request_market_depth(ticker)
            
            
    def print_watchlist(self):
        ticker_list = []
        
        for ticker in self.watchlist:
            ticker_list.append(ticker)
        
        print('')
        print(self.name())
        print(ticker_list)
        print('')
        
    
    
    def refresh_all(self):
        '''
        Once the summary has arrived ask for an update on the summary.  
        '''
        
        for listener in self._listeners:
            listener.conclude_summary()
        
        for ticker in self.watchlist:
            self.remove_market_depth(ticker)
            self.request_market_depth(ticker)

    def connect(self) -> None:
        """
        Call super.connect() and call make initialization requests.

        """
        super().connect()
        

    def _set_message_mappings(self) -> None:
        """Creates map of message processing functions."""
        super()._set_message_mappings()
        self._pf_dict['n'] = self._process_invalid_symbol
        self._pf_dict['P'] = self._process_summary
        self._pf_dict['Q'] = self._process_update
        
        # most depth message uses number instead of letter as their first field
        
        self._pf_dict['O'] = self.print_test
        self._pf_dict['0'] = self._process_price_level_order
        self._pf_dict['3'] = self._process_add_order
        self._pf_dict['4'] = self._process_order_update
        self._pf_dict['5'] = self._process_order_delete
        self._pf_dict['6'] = self._process_order_summary
        self._pf_dict['7'] = self._process_price_level_summary
        self._pf_dict['8'] = self._process_price_level_update
        self._pf_dict['9'] = self._process_price_level_delete
        self._pf_dict['M'] = self._process_market_maker_msg
        self._pf_dict['q'] = self._process_no_depth

        self._sm_dict["KEY"] = self._process_auth_key
        self._sm_dict["KEYOK"] = self._process_keyok
        self._sm_dict["CUST"] = self._process_customer_info
        self._sm_dict["WATCHES"] = self._process_watches
        self._sm_dict["CURRENT LOG LEVELS"] = self._process_current_log_levels
        self._sm_dict[
            "SYMBOL LIMIT REACHED"] = self._process_symbol_limit_reached
        self._sm_dict["IP"] = self._process_ip_addresses_used
        

    def print_test(self,fields):
        print('!!!!!!')
        print(fields)
        print('!!!!!!')
        

    def _process_invalid_symbol(self, fields: Sequence[str]) -> None:
        """Called when IQFeed tells us we used and invalid symbol."""
        assert len(fields) > 1
        assert fields[0] == 'n'
        bad_sym = fields[1]
        for listener in self._listeners:
            listener.process_invalid_symbol(bad_sym)


    def _process_summary(self, fields: Sequence[str]) -> None:
        """Process a symbol summary message"""
        assert len(fields) > 2
        assert fields[0] == "P"
        update = self._create_update(fields)
        for listener in self._listeners:
            listener.process_summary(update)

    def _process_update(self, fields: Sequence[str]) -> None:
        """Process a symbol update message."""
        assert len(fields) > 2
        assert fields[0] == "Q"
        update = self._create_update(fields)
        for listener in self._listeners:
            listener.process_update(update)

    def _create_update(self, fields: Sequence[str]) -> np.array:
        """Create an update message."""
        update = self._empty_update_msg
        for field_num, field in enumerate(fields[1:]):
            if field_num >= self._num_update_fields and field == "":
                break
            update[self._update_names[field_num]] = self._update_reader[
                field_num](field)
        return update

           

    def _process_auth_key(self, fields: Sequence[str]) -> None:
        """Still sent so needs to be handled, but obsolete."""
        assert len(fields) > 2
        assert fields[0] == "S"
        assert fields[1] == "KEY"
        auth_key = fields[2]
        for listener in self._listeners:
            listener.process_auth_key(auth_key)

    def _process_keyok(self, fields: Sequence[str]) -> None:
        """Still sent so needs to be handled, but obsolete."""
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == "KEYOK"
        for listener in self._listeners:
            listener.process_keyok()

    CustomerInfoMsg = namedtuple(
        "CustomerInfoMsg", (
            "svc_type", "ip_address", "port",
            "token", "version",
            "rt_exchanges", "max_symbols",
            "flags"))

    def _process_customer_info(self, fields: Sequence[str]) -> None:
        """Handle a customer information message."""
        assert len(fields) > 11
        assert fields[0] == 'S'
        assert fields[1] == "CUST"
        # noinspection PyCallByClass
        cust_info = DepthConn.CustomerInfoMsg(
            svc_type=(fields[2] == "real_time"),
            ip_address=fields[3],
            port=int(fields[4]),
            token=fields[5],
            version=fields[6],
            rt_exchanges=fields[8].split(" "),
            max_symbols=int(fields[10]),
            flags=fields[11])
        for listener in self._listeners:
            listener.process_customer_info(cust_info)

    def _process_watches(self, fields: Sequence[str]) -> None:
        """Handle a watches message."""
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == "WATCHES"
        
        for listener in self._listeners:
            listener.process_watched_symbols(fields[2:])

    def _process_current_log_levels(self, fields: Sequence[str]) -> None:
        """Called when IQFeed acknowledges log levels have changed"""
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == "CURRENT LOG LEVELS"
        for listener in self._listeners:
            listener.process_log_levels(fields[2:])

    def _process_symbol_limit_reached(self, fields: Sequence[str]) -> None:
        """Handle IQFeed telling us the symbol limit has been reached."""
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == "SYMBOL LIMIT REACHED"
        sym = fields[2]
        for listener in self._listeners:
            listener.process_symbol_limit_reached(sym)

    def _process_ip_addresses_used(self, fields: Sequence[str]) -> None:
        """IP addresses IQFeed.exe is connecting to for data."""
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'IP'
        ip = fields[2]
        for listener in self._listeners:
            listener.process_ip_addresses_used(ip)
 


    def request_watches(self) -> None:
        """
        Request a current watches message.

        IQFeed.exe will send you a list of all securities currently watched

        process_watched_symbols is called in each listener when the list of
        current watches message is received.
        """
        
        self._send_cmd("S,REQUEST WATCHES\r\n")

    def unwatch_all(self) -> None:
        """Unwatch all symbols."""
        self._send_cmd("S,UNWATCH ALL")
        
        
    """
    The processing function for the market depth messages
    """

    def _process_price_level_order(self, fields: Sequence[str]):
        """Process a symbol update message."""
        assert len(fields) > 2
        assert fields[0] == "0"
       
        for listener in self._listeners:
            listener.process_price_level_order(fields)
        
    
    
    def _process_add_order(self, fields: Sequence[str]):
        """Process a symbol update message."""
        assert len(fields) > 2
        assert fields[0] == "3"
       
        for listener in self._listeners:
            listener.process_add_order(fields)
    
    
    def _process_order_update(self, fields: Sequence[str]):
        '''
        After the summary is sent the ticker will start receiving updates. 
        Since we are only interested in the summary, we cancel the watch and re
        ask another summary in a loop. The reception of the first update message is the trigger. 
        '''
        """Process a symbol update message."""
        
        assert len(fields) > 2
        assert fields[0] == "4"
        
        
    
    
    def _process_order_delete(self, fields: Sequence[str]):
        """Process a symbol update message."""
        assert len(fields) > 2
        assert fields[0] == "5"
       
        for listener in self._listeners:
            listener.process_order_delete(fields)
    
    
    def _process_order_summary(self, fields: Sequence[str]):
        """Process a symbol update message."""
        assert len(fields) > 2
        assert fields[0] == "6"
        
        ticker = fields[1]
        self.watchlist[ticker]=True
       
        for listener in self._listeners:
            listener.process_order_summary(fields)
    
    
    def _process_price_level_summary(self, fields: Sequence[str]):
        """Process a symbol update message."""
        assert len(fields) > 2
        assert fields[0] == "7"
       
        for listener in self._listeners:
            listener.process_price_level_summary(fields)
    
    
    def _process_price_level_update(self, fields: Sequence[str]):
        """Process a symbol update message."""
        assert len(fields) > 2
        assert fields[0] == "8"
       
        for listener in self._listeners:
            listener.process_price_level_update(fields)
    
    
    def _process_price_level_delete(self, fields: Sequence[str]):
        """Process a symbol update message."""
        assert len(fields) > 2
        assert fields[0] == "9"
       
        for listener in self._listeners:
            listener.process_price_level_delete(fields)
    
    
    def _process_market_maker_msg(self, fields: Sequence[str]):
        """Process a symbol update message."""
        assert len(fields) > 2
        assert fields[0] == "M"
       
        for listener in self._listeners:
            listener.process_market_maker_msg(fields)
    
    
    def _process_no_depth(self, fields: Sequence[str]):
        """Process a symbol update message."""
        #assert len(fields) > 2
        assert fields[0] == "q"
       
        for listener in self._listeners:
            listener.process_no_depth(fields)


    def request_market_depth(self, symbol: str):
        self._send_cmd("WOR,%s\r\r\n" % symbol)
        
        """
        temps = t.time()
        
        self.time_summary = temps
        
        temps_between = temps-self.time_between
        print('between summary it tooks {} seconds'.format(temps_between))
        """
           
    
    def remove_market_depth(self,symbol: str):
        self._send_cmd("ROR,%s\r\r\n" % symbol)
        
        """
        temps = t.time()
        self.time_between = temps
        
        temps_summary = temps-self.time_summary
        print('calling and receiving the summary tooks {} seconds'.format(temps_summary))  
        """
        
    
    def request_market_depth_list(self):
        pass

        
        
        
        




def launch_service():
    """Check if IQFeed.exe is running and start if not"""

    svc = test_service.FeedService(product=dtn_product_id,
                         version="Debugging",
                         login=dtn_login,
                         password=dtn_password)
    svc.launch(headless=False)

    # If you are running headless comment out the line above and uncomment
    # the line below instead. This runs IQFeed.exe using the xvfb X Framebuffer
    # server since IQFeed.exe runs under wine and always wants to create a GUI
    # window.
    # svc.launch(headless=True)

#launch_service()



class DepthListener(VerboseIQFeedListener):
    """
    Verbose version of SilentQuoteListener.

    See documentation for SilentQuoteListener member functions.

    """
    
    #update_fields = update_fields = ['Most Recent Trade','Most Recent Trade Size','Most Recent Trade Time']
    

    def __init__(self, name,max_depth=10):
        super().__init__(name)
        
        self.max_depth=10
        self.dd = depth_data.depth_data(init=True)
        self.depth_dict = {}
        self.compress_dict={}
        self.update_dict = {}
        self.order_col = ['msg_type','symbol','order_id','MMID','side','price'
                      ,'order_size','order_priority','precision','time','date']
        
        self.processing=False
        
        self.no_depth_dict = {}
    
    
    def get_compress_dict(self):
        return self.compress_dict
        
    
    def conclude_summary(self):
        self.sort_depth_dict()
        self.save_compress_data()
       
        self.reset_depth_data()
        
        
        
    def reset_depth_data(self):
        self.depth_dict={}
        
    
    def show_depth_data(self):
        
        self.processing = True
        for ticker in self.depth_dict:
            
            print('!!! {} start {} !!!'.format(ticker,ticker))
            
            ask_list = self.depth_dict[ticker]['A']
            bid_list = self.depth_dict[ticker]['B']
            
            if len(ask_list)>len(bid_list):
                length = len(bid_list)
            else:
                length = len(ask_list)
                
                
            for x in range(length):
                ask = self.depth_dict[ticker]['A'][x]
                bid = self.depth_dict[ticker]['B'][x]
                
                ask_text = ask[3]+' '+str(ask[5])+' '+str(ask[6])+' '
                bid_text = bid[3]+' '+str(bid[5])+' '+str(bid[6])+' '
                
                print(bid_text+ask_text)
            
            print('!!! {} end {} !!!'.format(ticker,ticker))
            
        self.processing=False
            
        #self.show_compress_data()
     
        
    def save_compress_data(self):
        
        self.compress_depth_dict()
        
        ask_list = self.create_row_list('A')
        bid_list = self.create_row_list('B')
        
        #print('ask list: {}'.format(ask_list))
        #print('bid list: {}'.format(bid_list))
        
        table='ASK'
        self.dd.update_multiple_row(ask_list,table)
        
        table='BID'
        self.dd.update_multiple_row(bid_list,table)
        
        self.print_current_time()
       
        
        
        
    def create_row_list(self,side):
        '''
        Create either the Ask or Bid row list.
        '''
        
        row_list = []
        
        for ticker in self.compress_dict:
            count = 0
            #no_pk_ticker = ticker[:-3] # removing the .PK at the end
            
            for data in self.compress_dict[ticker][side]:    
 
                ind = count 
                mmid = data[3]
                price = data[5]
                size = data[6]
                
                #row = [no_pk_ticker,ind,mmid,price,size]
                row = [ticker,ind,mmid,price,size]
                row_list.append(row)
                
                #print(row)
                
                count = count + 1
                if count >= self.max_depth:
                    break
                
            '''
            Some ticker do not have a depth of 10. We need to fill these row.
            '''
            
            while count<self.max_depth:
                ind = count
                #row = [no_pk_ticker,ind,'NA',0,0]
                row = [ticker,ind,'NA',0,0]
                row_list.append(row)
                
                count = count + 1
                
        return row_list
            
        
        
            
    def show_compress_data(self):
        self.compress_depth_dict()
        
        for ticker in self.compress_dict:
        
           print('')
           print(ticker)
           print('!!! {} compress start {} !!!'.format(ticker,ticker))
           
           ask_list = self.compress_dict[ticker]['A']
           bid_list = self.compress_dict[ticker]['B']
           
           if len(ask_list)>len(bid_list):
               length = len(bid_list)
           else:
               length = len(ask_list)
               
               
           for x in range(length):
               ask = self.compress_dict[ticker]['A'][x]
               bid = self.compress_dict[ticker]['B'][x]
               
               ask_text = ask[3]+' '+str(ask[5])+' '+str(ask[6])+' '
               bid_text = bid[3]+' '+str(bid[5])+' '+str(bid[6])+' '
               
               print(bid_text+ask_text)
           
           print('!!! {} compress end {} !!!'.format(ticker,ticker))
           print('')
        
            
        
    
    def get_depth_dict(self):
        return self.depth_dict
    

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        print("%s: Invalid Symbol: %s" % (self._name, bad_symbol))
        self.dwl = depth_watchlist.depth_watchlist(init=False,active_liquid=False)
        self.dwl.invalidate_ticker(bad_symbol)
        
        


    def process_price_level_order(self, fields: Sequence[str]):
        assert fields[0] == "0"
        print('price level order')
        print(fields)
        
    
    
    def process_add_order(self, fields: Sequence[str]):
        assert fields[0] == "3"
        print('*******')
        print('*******')
        print('*******')
        print('*******')
        print('*******')
        print('add order')
        raise Exception('finaly an add order !!!!')
        
       
    
    def process_order_update(self, fields: Sequence[str]):
        '''
        The order update is not needed for this particular solution
        '''
        assert fields[0] == "4"

    
    
    def process_order_delete(self, fields: Sequence[str]):
        
        '''
        So far the order delete seems to be only used to indicate the end
        of a summary message. 
        '''
        
        assert fields[0] == "5"
              
    
    
    def process_order_summary(self, fields: Sequence[str]):
        '''
        Since we do not know when the summary message end we have to sort the
        data everytime we receive a message. 
        '''
        
        assert fields[0] == "6"
        ticker = fields[1]
        
        if self.processing==False:
            if ticker not in self.depth_dict:
                self.depth_dict[ticker]={}
                self.depth_dict[ticker]['A']=[]
                self.depth_dict[ticker]['B']=[]
                
            side = fields[4]
            
            price = fields[5]
            size = fields[6]
            fields[5] = float(price)
            fields[6] = int(size)
            
            self.depth_dict[ticker][side].append(fields)
        else:
            print('Processing the depth dict cannot process the incoming data')
        
    
        
    
    def compress_depth_dict(self):
        
        self.compress_dict = {}
        
        self.processing=True
        for ticker in self.depth_dict:
            self.compress_dict[ticker]={}    
            self.compress_dict[ticker]['A']=[]
            self.compress_dict[ticker]['B']=[]
            
            ask_list = self.depth_dict[ticker]['A']
            total_size=0
            
            for x in range(len(ask_list)):
                price = ask_list[x][5]
                size = ask_list[x][6]
                if x==0:
                    last_price = price
                    
                    index = 0
                    total_size = size
                else:
                    
                    if price == last_price:
                        total_size = total_size+size
                    else:
                        first_row = ask_list[index].copy()
                        first_row[6]=total_size
                        
                        self.compress_dict[ticker]['A'].append(first_row)
                        
                        index = x
                        total_size = size
                    
                last_price = price
            
            bid_list = self.depth_dict[ticker]['B']
            total_size =0
            
            for x in range(len(bid_list)):
                price = bid_list[x][5]
                size = bid_list[x][6]
                
                if x==0:
                    last_price = price
                    
                    index = 0 
                    total_size = size
                else:
                    
                    
                    if price == last_price:
                        total_size = total_size+size
                    else:
                        first_row = bid_list[index].copy()
                        first_row[6]=total_size
                        
                        self.compress_dict[ticker]['B'].append(first_row)
                        
                        index = x
                        total_size = size
                    
                last_price = price
                     
            self.processing=False
        
        
    def sort_depth_dict(self):
        
        for ticker in self.depth_dict:
            ask_list = self.depth_dict[ticker]['A']
            bid_list = self.depth_dict[ticker]['B']
            
            self.depth_dict[ticker]['A'] = sorted(ask_list,key=lambda x: x[5]) 
            self.depth_dict[ticker]['B'] = sorted(bid_list,key=lambda x: x[5],reverse=True) 
            
     
    def print_current_time(self):
        today = datetime.datetime.today()
        hour = today.hour
        minute = today.minute
        second = today.second
        if minute < 10:
            minute = '0'+str(minute)
        if second< 10:
            second = '0'+str(second)
            
        temps = '{}:{}:{}'.format(hour,minute,second)
        print('Depth last update: {}'.format(temps))
        print('')
    
    
    def process_price_level_summary(self, fields: Sequence[str]):
        assert fields[0] == "7"
        print('price level summary')
        print(fields)
        pass
    
    
    def process_price_level_update(self, fields: Sequence[str]):
        assert fields[0] == "8"
        print('price level update')
        print(fields)
        pass
    
    
    def process_price_level_delete(self, fields: Sequence[str]):
        assert fields[0] == "9"
        print('price level delete')
        print(fields)
        pass
    
    
    def process_market_maker_msg(self, fields: Sequence[str]):
        assert fields[0] == "M"
        print('market maker msg')
        print(fields)
        pass
    
    
    def process_no_depth(self, fields: Sequence[str]):
        assert fields[0] == "q"
        ticker = fields[1]
        if ticker not in self.no_depth_dict:
            self.no_depth_dict[ticker] = True
        
    
    def print_no_depth(self):
        ticker_list = []
        for ticker in self.no_depth_dict:
            ticker_list.append(ticker)
        
        print('')
        print('no depth list')
        print(ticker_list)
        print('')
            
    

    def process_auth_key(self, key: str) -> None:
        print("%s: Authorization Key Received: %s" % (self._name, key))

    def process_keyok(self) -> None:
        print("%s: Authorization Key OK" % self._name)

    def process_customer_info(self,
                              cust_info: DepthConn.CustomerInfoMsg) -> None:
        print("%s: Customer Information:" % self._name)
        print(cust_info)

    def process_watched_symbols(self, symbols: Sequence[str]):
        print("%s: List of subscribed symbols:" % self._name)
        print(symbols)

    def process_log_levels(self, levels: Sequence[str]) -> None:
        print("%s: Active Log levels:" % self._name)
        print(levels)

    def process_symbol_limit_reached(self, sym: str) -> None:
        print("%s: Symbol Limit Reached with subscription to %s" %
              (self._name, sym))

    def process_ip_addresses_used(self, ip: str) -> None:
        print("%s: IP Addresses Used: %s" % (self._name, ip))
        
    def process_timestamp(self, time_val: FeedConn.TimeStampMsg):
        #print("%s: Timestamp:" % self._name)
        #print(time_val)
        pass
        




"""Get level 2 quotes"""


timer=4.0 # 4 seconds is needed between requests to process the summary of the depth data. 

launch_service()


constant_conn = DepthConn(name='constant conn')
depth_conn = DepthConn(name="depth_conn")
listener = DepthListener("Level 2 Listener")
depth_conn.add_listener(listener)

with iq.ConnConnector([constant_conn]) as connector:
    with iq.ConnConnector([depth_conn]) as connector:
        
        constant_conn.start_watchlist()
        t.sleep(0.1)
        depth_conn.start_watchlist()
        t.sleep(0.1)
        
        today = datetime.datetime.today()
        hour = today.hour
        
        count = 0
        last_time = t.time()
        while hour<16:
            
            current_time = t.time()
            delta_time = current_time-last_time
            
            if delta_time >=timer:
                
                last_time = t.time()
                depth_conn.refresh_all()
                count = count + 1
                
                constant_conn.update_watchlist()
                depth_conn.update_watchlist()
                
            
            if count >=5:
                count = 0
                
                listener.print_no_depth()
                depth_conn.print_watchlist()
            
            
            today = datetime.datetime.today()
            hour = today.hour
        
        constant_conn.unwatch_all()
        depth_conn.unwatch_all()
        depth_conn.remove_listener(listener)        
    
 
    
depth_dict = listener.get_depth_dict()    
compress_dict = listener.get_compress_dict()        

       
   
    

        
        
        
#end        
        
        
        
        
        