
'''
Note! most of the code of this module come from https://github.com/akapur/pyiqfeed 
Some modifications had to be made in order to download the summary. 
'''


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


import os
import datetime
import itertools
import select
import socket
import threading
import time as t
import pandas as pd 
import date_manager 

from collections import deque, namedtuple
from typing import Sequence, List
import xml.etree.ElementTree as ElementTree

import numpy as np
from pyiqfeed.exceptions import NoDataError, UnexpectedField, UnexpectedMessage
from pyiqfeed.exceptions import UnexpectedProtocol, UnauthorizedError
from pyiqfeed import field_readers as fr


# let see if we can make it work. 
from localconfig.passwords import dtn_product_id, dtn_login, dtn_password
import pyiqfeed as iq
from pyiqfeed import VerboseIQFeedListener
import test_service


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












class LookupConn(FeedConn):
    """
    MarketConn lets you look at the end of day summary of different market.

    Like HistoryConn the function called returns the data. Like HistoryConn
    you do receive messages from this class if you add a listener. These
    messages are about the connection itself and it's safe not to listen
    for them. If you are having trouble of some sort with this class, please
    first add a listener and see if one of the messages tells you something
    about why it's not working before assuming things don't work.

    """

    host = FeedConn.host
    port = FeedConn.lookup_port

    futures_month_letter_map = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K',
                                6: 'M', 7: 'N', 8: 'Q', 9: 'U', 10: 'V',
                                11: 'X', 12: 'Z'}
    futures_month_letters = (
            'F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z')

    call_month_letters = (
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L')
    call_month_letter_map = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E',
                             6: 'F', 7: 'G', 8: 'H', 9: 'I', 10: 'J',
                             11: 'K', 12: 'L'}

    put_month_letters = (
            'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X')
    put_month_letter_map = {1: 'M', 2: 'N', 3: 'O', 4: 'P', 5: 'Q',
                            6: 'R', 7: 'S', 8: 'T', 9: 'U', 10: 'V',
                            11: 'W', 12: 'X'}

    asset_type = np.dtype(
            [('symbol', 'S128'), ('market', 'u1'), ('security_type', 'u1'),
             ('name', 'S128'), ('sector', 'u8')])

    def __init__(self, name: str = "SymbolSearchConn",
                 host: str = FeedConn.host, port: int = port):
        super().__init__(name, host, port)
        self._set_message_mappings()
        self._req_num = 0
        self._req_buf = {}
        self._req_numlines = {}
        self._req_event = {}
        self._req_failed = {}
        self._req_err = {}
        self._req_lock = threading.RLock()

    def _set_message_mappings(self) -> None:
        super()._set_message_mappings()
        self._pf_dict['L'] = self._process_lookup_datum

    def _send_connect_message(self):
        # The history/lookup socket does not accept connect messages
        pass

    def _process_lookup_datum(self, fields: Sequence[str]) -> None:
        req_id = fields[0]
        if 'E' == fields[1]:
            # Error
            self._req_failed[req_id] = True
            err_msg = "Unknown Error"
            if len(fields) > 2:
                if fields[2] != "":
                    err_msg = fields[2]
            self._req_err[req_id] = err_msg
        elif '!ENDMSG!' == fields[1]:
            #print(fields[1])
            self._req_event[req_id].set()
        else:
            self._req_buf[req_id].append(fields)
            self._req_numlines[req_id] += 1

    def _get_next_req_id(self) -> str:
        with self._req_lock:
            req_id = "L_%.10d" % self._req_num
            self._req_num += 1
            return req_id

    def _cleanup_request_data(self, req_id: str) -> None:
        with self._req_lock:
            del self._req_failed[req_id]
            del self._req_err[req_id]
            del self._req_buf[req_id]
            del self._req_numlines[req_id]

    def _setup_request_data(self, req_id: str) -> None:
        with self._req_lock:
            self._req_buf[req_id] = deque()
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_err[req_id] = ""
            self._req_event[req_id] = threading.Event()

    def _get_data_buf(self, req_id: str) -> FeedConn.databuf:
        """Get the data buffer for a specific request."""
        with self._req_lock:
            buf = FeedConn.databuf(
                    failed=self._req_failed[req_id],
                    err_msg=self._req_err[req_id],
                    num_pts=self._req_numlines[req_id],
                    raw_data=self._req_buf[req_id])
        self._cleanup_request_data(req_id)
        return buf
    
    
    
    
    def _read_eds_msg(self, req_id: str) -> np.array:
        """Get a data buffer and turn into np array of dtype asset_type."""
        res = self._get_data_buf(req_id)
        if res.failed:
            return np.array([res.err_msg], dtype='object')
        else:
            data = []
            line_num = 0
            while res.raw_data and (line_num < res.num_pts):
                dl = res.raw_data.popleft()
                
                data.append(dl)
                
                line_num += 1
                if line_num >= res.num_pts:
                    assert len(res.raw_data) == 0
                if len(res.raw_data) == 0:
                    assert line_num >= res.num_pts
            return data


    def _request_eds(self, security_type,market,date,timeout=None):
        """
        Search request the end of day summary for a type of security from
        a specific market.

        :param search_term: an integer. 1 represent equities. 
        :param market: an integer. For example 6 represent NYSE American
        :param date: yearmonthday  for example 20220309
      
        :return: np.array of dtype LookupConn.asset_type

        EDS,[Security Type],[Group ID],[Date],[RequestID]<CR><LF>

        SBF,[Field To Search],[Search String],[Filter Type],[Filter Value],
        [RequestID]<CR><LF>

        """

        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        
        
        req_cmd ='EDS,%s,%s,%s,%s\r\n' %(str(security_type),str(market),str(date),str(req_id))
        
    
        self._send_cmd(req_cmd)
      
        self._req_event[req_id].wait(timeout=timeout)
     
        
        data = self._read_eds_msg(req_id)
      
        return data
    
    
    
    def _request_fds(self, security_type,market,date,timeout=None):
        """
        Request a fundamental summary for all symbols in a security type and
        exchange group.

        :param search_term: an integer. 1 represent equities. 
        :param market: an integer. For example 6 represent NYSE American
        :param date: yearmonthday  for example 20220309
      
        :return: np.array of dtype LookupConn.asset_type

        EDS,[Security Type],[Group ID],[Date],[RequestID]<CR><LF>

        SBF,[Field To Search],[Search String],[Filter Type],[Filter Value],
        [RequestID]<CR><LF>

        """

        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        
       
        req_cmd ='FDS,%s,%s,%s,%s\r\n' %(str(security_type),str(market),str(date),str(req_id))
        
       
        
        self._send_cmd(req_cmd)
      
        self._req_event[req_id].wait(timeout=timeout)
   
        
        data = self._read_eds_msg(req_id)
      
        return data
        
    
    
    
    

    def _read_symbols(self, req_id: str) -> np.array:
        """Get a data buffer and turn into np array of dtype asset_type."""
        res = self._get_data_buf(req_id)
        if res.failed:
            return np.array([res.err_msg], dtype='object')
        else:
            data = np.empty(res.num_pts, LookupConn.asset_type)
            line_num = 0
            while res.raw_data and (line_num < res.num_pts):
                dl = res.raw_data.popleft()
                data[line_num]['symbol'] = dl[1].strip()
                data[line_num]['market'] = fr.read_uint8(dl[2])
                data[line_num]['security_type'] = fr.read_uint8(dl[3])
                data[line_num]['name'] = dl[4].strip()
                data[line_num]['sector'] = 0
                line_num += 1
                if line_num >= res.num_pts:
                    assert len(res.raw_data) == 0
                if len(res.raw_data) == 0:
                    assert line_num >= res.num_pts
            return data


    def request_symbols_by_filter(self, search_term: str,
                                  search_field: str = 'd', filt_val: str =
                                  None,
                                  filt_type: str = None,
                                  timeout=None) -> np.array:
        """
        Search for symbols and return matches.

        :param search_term: 's': search symbols, 'd': search descriptions.
        :param search_field: What to search for.
        :param filt_val: 'e': Space delimited list of markets of security types.
        :param filt_type: Specific markets, 't': Specific security types.
        :param timeout: Must return before timeout or die, Default None
        :return: np.array of dtype LookupConn.asset_type

        SBF,[Field To Search],[Search String],[Filter Type],[Filter Value],
        [RequestID]<CR><LF>

        """
        assert search_field in ('d', 's')
        assert search_term is not None
        assert filt_type is None or filt_type in ('e', 't')

        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "SBF,%s,%s,%s,%s,%s\r\n" % (
            search_field, search_term, fr.blob_to_str(filt_type),
            fr.blob_to_str(filt_val), req_id)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_symbols(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def _read_symbols_with_sect(self, req_id: str) -> np.array:
        """Read symbols from buffer where sector field is not null."""
        res = self._get_data_buf(req_id)
        if res.failed:
            return np.array([res.err_msg], dtype='object')
        else:
            data = np.empty(res.num_pts, LookupConn.asset_type)
            line_num = 0
            while res.raw_data and (line_num < res.num_pts):
                dl = res.raw_data.popleft()
                data[line_num]['sector'] = fr.read_uint64(dl[1])
                data[line_num]['symbol'] = dl[2].strip()
                data[line_num]['market'] = fr.read_uint8(dl[3])
                data[line_num]['security_type'] = fr.read_uint8(dl[4])
                data[line_num]['name'] = dl[5].strip()
                line_num += 1
                if line_num >= res.num_pts:
                    assert len(res.raw_data) == 0
                if len(res.raw_data) == 0:
                    assert line_num >= res.num_pts
            return data

    def request_symbols_by_sic(self, sic: int, timeout=None) -> np.array:
        """
        Return symbols in a specific SIC sector.

        :param sic: SIC number for sector
        :param timeout: Wait timeout secs for data or die. Default None
        :return: np.array of dtype LookupConn.asset_type

        SBS,[Search String],[RequestID]<CR><LF>

        """
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "SBS,%d,%s\r\n" % (sic, req_id)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_symbols_with_sect(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_symbols_by_naic(self, naic: int, timeout=None) -> np.array:
        """
        Return symbols in a specific NAIC sector.

        :param naic: SIC number for sector
        :param timeout: Wait timeout secs for data or die. Default None
        :return: np.array of dtype LookupConn.asset_type

        SBN,[Search String],[RequestID]<CR><LF>

        """
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "SBS,%d,%s\r\n" % (naic, req_id)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_symbols_with_sect(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def _read_futures_chain(self, req_id: str) -> List[str]:
        """Read a buffer and return it as a futures chain."""
        res = self._get_data_buf(req_id)
        if res.failed:
            return ["!ERROR!", res.err_msg]
        else:
            assert res.num_pts == 1
            chain = res.raw_data[0][1:]
            if chain[-1] == "":
                chain = chain[:-1]
            return chain

    def request_futures_chain(self, symbol: str, month_codes: str = None,
                              years: str = None, near_months: int = None,
                              timeout: int = None) -> List[str]:
        """
        Request a futures chain
        :param symbol: Underlying symbol.
        :param month_codes: String containing month codes we want.
        :param years: Example: 2005 - 2014 would be "5678901234".
        :param near_months: Number of near months (ignore months and years).
        :param timeout: Die after timeout seconds, Default None.
        :return: List of futures tickers.

        CFU,[Symbol],[Month Codes],[Years],[Near Months],[RequestID]<CR><LF>

        """
        assert (symbol is not None) and (symbol != '')

        assert month_codes is None or near_months is None
        assert month_codes is not None or near_months is not None

        if month_codes is not None:
            # noinspection PyTypeChecker
            for month_code in month_codes:
                assert month_code in LookupConn.futures_month_letters

        if years is not None:
            assert years.isdigit()

        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "CFU,%s,%s,%s,%s,%s\r\n" % (
            symbol, fr.blob_to_str(month_codes), fr.blob_to_str(years),
            fr.blob_to_str(near_months), req_id)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_futures_chain(req_id)
        if (len(data) == 2) and (data[0] == "!ERROR!"):
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[1]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_futures_spread_chain(
            self,
            symbol: str,
            month_codes: str = None,
            years: str = None,
            near_months: int = None,
            timeout: int = None) -> List[str]:
        """
        Request a chain of futures spreads
        :param symbol: Underlying symbol.
        :param month_codes: String containing month codes we want.
        :param years: Example: 2005 - 2014 would be "5678901234".
        :param near_months: Number of near months (ignore months and years).
        :param timeout: Die after timeout seconds, Default None.
        :return: List of futures spread tickers.

        CFS,[Symbol],[Month Codes],[Years],[Near Months],[RequestID]<CR><LF>

        """
        assert (symbol is not None) and (symbol != '')

        assert month_codes is None or near_months is None
        assert month_codes is not None or near_months is not None

        if month_codes is not None:
            # noinspection PyTypeChecker
            for month_code in month_codes:
                assert month_code in LookupConn.futures_month_letters

        if years is not None:
            assert years.isdigit()

        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "CFS,%s,%s,%s,%s,%s\r\n" % (
            symbol, fr.blob_to_str(month_codes), fr.blob_to_str(years),
            fr.blob_to_str(near_months), req_id)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_futures_chain(req_id)
        if (len(data) == 2) and (data[0] == "!ERROR!"):
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[1]))
            raise RuntimeError(err_msg)
        else:
            return data

    def _read_option_chain(self, req_id: str):
        res = self._get_data_buf(req_id)
        if res.failed:
            return ["!ERROR!", res.err_msg]
        else:
            assert res.num_pts == 1
            symbols = res.raw_data[0][1:]
            cp_delim = symbols.index(':')
            call_symbols = symbols[:cp_delim]
            if len(call_symbols) > 0:
                if call_symbols[-1] == "":
                    call_symbols = call_symbols[:-1]
            put_symbols = symbols[cp_delim + 1:]
            if len(put_symbols) > 0:
                if put_symbols[-1] == "":
                    put_symbols = put_symbols[:-1]
            return {"c": call_symbols, "p": put_symbols}

    def request_futures_option_chain(self, symbol: str, opt_type: str = 'pc',
                                     month_codes: str = None, years: str =
                                     None,
                                     near_months: int = None,
                                     timeout: int = None) -> dict:
        """
        Request a chain of options on futures contracts.

        :param symbol: Underlying symbol of the futures contract.
        :param opt_type: 'p'=Puts, 'c'=Calls, 'pc'=Both
        :param month_codes: String of months you want
        :param years: Example: 2005 - 2014 would be "5678901234".
        :param near_months: Number of near months (ignore months and years)
        :param timeout: Die after timeout secs, Default None.
        :return: List of Options tickers
        CFO,[Symbol],[Puts/Calls],[Month Codes],[Years],[Near Months],
        [RequestID]<CR><LF>

        """
        assert (symbol is not None) and (symbol != '')

        assert opt_type is not None
        assert len(opt_type) in (1, 2)
        for op in opt_type:
            assert op in ('p', 'c')

        assert month_codes is None or near_months is None
        assert month_codes is not None or near_months is not None

        if month_codes is not None:
            valid_month_codes = ()
            if opt_type == 'p':
                valid_month_codes = LookupConn.put_month_letters
            elif opt_type == 'c':
                valid_month_codes = LookupConn.call_month_letters
            elif opt_type == 'cp' or opt_type == 'pc':
                valid_month_codes = (
                    LookupConn.call_month_letters +
                    LookupConn.put_month_letters)
            # noinspection PyTypeChecker
            for month_code in month_codes:
                assert month_code in valid_month_codes

        if years is not None:
            assert years.isdigit()

        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "CFO,%s,%s,%s,%s,%s,%s\r\n" % (
            symbol,
            opt_type,
            fr.blob_to_str(month_codes),
            fr.blob_to_str(years),
            fr.blob_to_str(near_months),
            req_id)

        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_option_chain(req_id)
        if (type(data) == list) and (data[0] == "!ERROR!"):
            iqfeed_err = str(data[1])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == "!NO_DATA!":
                raise NoDataError(err_msg)
            elif iqfeed_err == "Unauthorized user ID.":
                raise UnauthorizedError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_equity_option_chain(self, symbol: str, opt_type: str = 'pc',
                                    month_codes: str = None,
                                    near_months: int = None,
                                    include_binary: bool = True,
                                    filt_type: int = 0,
                                    filt_val_1: float = None,
                                    filt_val_2: float = None,
                                    timeout: int = None) -> dict:
        """
        Request a chain of options on an equity.

        :param symbol: Underlying symbol of the stock.
        :param opt_type: 'p'=Puts, 'c'=Calls, 'pc'=Both.
        :param month_codes: String of months you want.
        :param near_months: Number of near months (ignore months).
        :param include_binary: Include binary options.
        :param filt_type: 0=No filter 1=strike_range, 2=In/Out of money.
        :param filt_val_1: Lower strike or Num contracts in the money.
        :param filt_val_2: Upper string or Num Contracts out of the money.
        :param timeout: Die after timeout secs, Default None.
        :return: List of Options tickers.

        CEO,[Symbol],[Puts/Calls],[Month Codes],[Near Months],
        [BinaryOptions],[Filter Type],[Filter Value One],[Filter Value Two],
        [RequestID]<CR><LF>

        """
        assert (symbol is not None) and (symbol != '')

        assert opt_type is not None
        assert len(opt_type) in (1, 2)
        for op in opt_type:
            assert op in ('p', 'c')

        assert month_codes is None or near_months is None
        assert month_codes is not None or near_months is not None

        if month_codes is not None:
            valid_month_codes = ()
            if opt_type == 'p':
                valid_month_codes = LookupConn.put_month_letters
            elif opt_type == 'c':
                valid_month_codes = LookupConn.call_month_letters
            elif opt_type == 'cp' or opt_type == 'pc':
                valid_month_codes = (
                    LookupConn.call_month_letters +
                    LookupConn.put_month_letters)
            # noinspection PyTypeChecker
            for month_code in month_codes:
                assert month_code in valid_month_codes
        assert filt_type in (0, 1, 2)
        if filt_type != 0:
            assert filt_val_1 is not None and filt_val_1 > 0
            assert filt_val_2 is not None and filt_val_2 > 0
        if filt_type == 1:
            assert filt_val_1 < filt_val_2
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "CEO,%s,%s,%s,%s,%d,%d,%s,%s,%s\r\n" % (
            symbol, opt_type, fr.blob_to_str(month_codes),
            fr.blob_to_str(near_months), include_binary, filt_type,
            fr.blob_to_str(filt_val_1), fr.blob_to_str(filt_val_2), req_id)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_option_chain(req_id)
        if (type(data) == list) and (data[0] == "!ERROR!"):
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[1]))
            raise RuntimeError(err_msg)
        else:
            return data




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




def request_eds_data(market,date,security_type=1,data_struct='df'):

    launch_service()
    
    """Lookup market summary."""
    lookup_conn = LookupConn(name="Market_lookup")
    lookup_listener = iq.VerboseIQFeedListener("TickerLookupListener")
    lookup_conn.add_listener(lookup_listener)
    
    with iq.ConnConnector([lookup_conn]) as connector:
        
       
        
        eds_data = lookup_conn._request_eds(security_type,market,date,timeout=None)
        
        #fds_data = lookup_conn._request_fds(security_type, market, date)
    
        lookup_conn.remove_listener(lookup_listener)
        
    
    if data_struct=='df':
        col_eds = eds_data[0]
        data_eds = eds_data[1:]   
        
        df_eds = pd.DataFrame(data_eds,columns=col_eds)
        
        return df_eds
    else:
        return eds_data
    
    
def request_fds_data(market,date,security_type=1,data_struct='df'):

    launch_service()
    
    """Lookup market summary."""
    lookup_conn = LookupConn(name="Market_lookup")
    lookup_listener = iq.VerboseIQFeedListener("TickerLookupListener")
    lookup_conn.add_listener(lookup_listener)
    
    with iq.ConnConnector([lookup_conn]) as connector:
        
       
        
        #eds_data = lookup_conn._request_eds(security_type,market,date,timeout=None)
        
        fds_data = lookup_conn._request_fds(security_type, market, date)
    
        lookup_conn.remove_listener(lookup_listener)
        
    
    if data_struct=='df':
        col_fds = fds_data[0]
        data_fds = fds_data[1:] 
        
        length = len(col_fds)

        data = []
        for row in data_fds:
            if len(row)==length:
                data.append(row)
        
        df_fds = pd.DataFrame(data,columns=col_fds)
        
        return df_fds
    else:
        return fds_data
    


def request_summary_data(market,date,security_type=1):
    
    '''
    nasdaq: 5 (nasdaq is exchange 1,2 and the otc market is exchange 3 wierdly enough)
    nyse_american: 6
    nyse:7
    nyse arca: 11, specialized in special trading product. 
    toronto stock exchange: 50
    '''
    
    df_eds = request_eds_data(market,date)
    
    df_fds = request_fds_data(market, date)
    
    """
    if with_market_cap:
        '''
        This function assumes that the index for the stock are the same
        in eds and fds data. 
        '''
        
        df_fds =df_fds.loc[df_fds['MarketCap']!='']
    """
    
    index_list = df_fds.index
    df_eds = df_eds.iloc[index_list]
        
        
    return df_eds,df_fds


def remove_extra_letter(df,letters =['F']):

    '''
    Only keep symbol with more then 4 letter from the letter list.
    '''
    
    
    for index,row in df.iterrows():
        ticker = row['Symbol']
        
        
        if len(ticker)>4:
            fifth_letter = ticker[4]
            if fifth_letter not in letters:
                df.drop(index,inplace=True)
                
                
def remove_duplicate(df):
    
    '''
    When two duplicate are found it seems to be always the case that
    the ticker with 5 letter is the wrong one.
    '''
    
    first=True
    duplicate_count = 0
    #drop_index = []
    for index,row in df.iterrows():
        
        ticker = row['Symbol']
          
        if first:
            first=False
            previous_ticker = ticker
            previous_index = index
        else:
            
            sliced_ticker = ticker[0:4]
            sliced_previous = previous_ticker[0:4]
            
            if sliced_ticker==sliced_previous:
                
                duplicate_count +=1
                
                if len(ticker)<=4:
                    df.drop(previous_index,inplace=True)
                    #drop_index.append(previous_index)
                    #print('dropped {} and kept {}'.format(previous_ticker,ticker))
                elif len(previous_ticker)<=4:
                    df.drop(index,inplace=True)
                    #drop_index.append(index)
                    #print('dropped {} and kept {}'.format(ticker,previous_ticker))
                
            
            
        previous_index = index
        previous_ticker = ticker
        
        
def create_summary_date():
    dh  = date_manager.date_handler()
    last_market_date = dh.get_last_market_date()
    
    year = str(last_market_date.year)
    month = last_market_date.month
    day = last_market_date.day
    
    if month<10:
        month = '0'+str(month)
    else:
        month = str(month)
        
    if day<10:
        day = '0'+str(day)
    else:
        day = str(day)
        
    date = year+month+day
    return date



def request_otc_summary(date,with_market_cap=False):

    market = 5 # note the true exchange is 3 but we need to request 5.    
    df_eds,df_fds = request_summary_data(market,date,with_market_cap)
    
    
    temp_df = df_eds.loc[df_eds['Exchange']=='3']
    df = temp_df.copy() # preventing an annoying warning. 
    
    '''
    There are a lot of duplicate from the otc market. 
    '''
    remove_extra_letter(df)
    remove_duplicate(df)
    
    index_list = df.index
    df_fds = df_fds.iloc[index_list]  
    
    df_eds = df
    
    return df_eds,df_fds 



def request_american_summary(date):

    market = 6     
    df_eds,df_fds = request_summary_data(market,date)
    
    #remove_extra_letter(df_eds)
    
    #index_list = df_eds.index
    #df_fds = df_fds.iloc[index_list]  
    
    
    return df_eds,df_fds    


# verify if the symbols matches
def verify_symbol_matches(dict_eds,dict_fds):
    counta = 0
    ticker_list = []
    for ticker in dict_fds:
        if ticker not in dict_eds:
            ticker_list.append(ticker)
            counta = counta + 1
            
    countb = 0
    for ticker in dict_eds:
        if ticker not in dict_fds:
            ticker_list.append(ticker)
            countb = countb + 1
            
    total_count = counta+countb
    
    print('The summaries contains {} mismatch'.format(len(ticker_list)))
    print(ticker_list)
    if (total_count/len(dict_eds))>0.05:
        raise Exception('Too many mistake with the summary')
        

def create_stock_info(df_eds,df_fds,ticker_list):
     
    stock_dict = {}
    for ticker in ticker_list:
        stock_dict[ticker]={}
        stock_dict[ticker]['name']=''
        stock_dict[ticker]['exchange']=0
        stock_dict[ticker]['sec_type']=1
        stock_dict[ticker]['common_share']=0
        stock_dict[ticker]['institutionalPercent']=0.0
        stock_dict[ticker]['shortInterest']=0.0  
        stock_dict[ticker]['close']=0.0
        stock_dict[ticker]['average_cash']=0.0
        stock_dict[ticker]['market_cap']=0.0
       
    for index,row in df_eds.iterrows():
        ticker = row['Symbol']
        
        if ticker in stock_dict:
            exchange = row['Exchange']
            sec_type = row['Type']
            
            volume = row['Volume']
            if volume !='':
                volume = float(volume)
            else:
                volume = 0
                
            close = row['Close']
            if close !='':
                close = float(close)
            else:
                close = 0
                
            avg_cash = volume*close
            
            stock_dict[ticker]['close']=close
            stock_dict[ticker]['exchange']=int(exchange)
            stock_dict[ticker]['sec_type']=int(sec_type)
            stock_dict[ticker]['average_cash']=avg_cash
                
            
    for index,row in df_fds.iterrows():
        ticker = row['Symbol']
        
        if ticker in stock_dict:
            name = row['Description']
            stock_dict[ticker]['name']=name
            
            ins_percent = row['InstitutionalPercent']
            if ins_percent!='':
                stock_dict[ticker]['institutionalPercent']=float(ins_percent)
                
            common_share = row['CommonSharesOutstanding']
            if common_share !='':
                stock_dict[ticker]['common_share']= int(common_share)*1000
                
            shortInterest = row['ShortInterest']
            if shortInterest !='':
                stock_dict[ticker]['shortInterest']=float(shortInterest)
                
            stock_dict[ticker]['market_cap']=stock_dict[ticker]['close']*stock_dict[ticker]['common_share']
               
            
    stock_df = pd.DataFrame.from_dict(stock_dict,orient='index')     
    stock_df.index.name='ticker'
    
    return stock_df


def request_summary(date,with_market_cap=True,mc_limit=200000000,min_cash=1000000):

    '''
    This method combine the summary of the nasdaq and nyse american.
    '''   
    
    market_identifier = [5,6] # the nasdaq and otc are under 5.
    
    market_info=[]
    for market in market_identifier:
        df_eds,df_fds = request_summary_data(market,date)
        ticker_list = df_eds['Symbol'].to_list()
        stock_info = create_stock_info(df_eds,df_fds,ticker_list)
        
        market_info.append(stock_info)
      
    stock_df = pd.concat(market_info,ignore_index=False)
    
    
    '''
    Drop row where exchange is not in exchange list. 
    '''
    
    # Exchange number: 1,2 represent the nasdaq andd 6 the nyse american
    exchange_list = [1,2,6]  
    
    stock_list = stock_df[stock_df['exchange'].isin(exchange_list)]
    
    if with_market_cap:
        stock_list = stock_list.loc[stock_list['market_cap']>0]
    
    stock_list = stock_list.loc[stock_list['market_cap']<mc_limit]
    stock_list = stock_list.loc[stock_list['average_cash']>min_cash]
        
    return stock_list






# end