# -*- coding: utf-8 -*-
"""
Created on Wed Nov 10 10:18:21 2021

@author: marti
"""

import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui

class cash_graph(pg.GraphicsObject):
    def __init__(self, data):
        pg.GraphicsObject.__init__(self)
        self.data = data  ## data must have fields: time, average_cash
        self.generatePicture()

    def generatePicture(self):
        ## pre-computing a QPicture object allows paint() to run much more quickly,
        ## rather than re-drawing the shapes every time.
        self.picture = QtGui.QPicture()
        p = QtGui.QPainter(self.picture)
        p.setPen(pg.mkPen('w'))
        
        if len(self.data)>=2:
            w = (self.data[1][0] - self.data[0][0]) / 3.
        else:
            w = 1/3
            
        for (t, avg_cash) in self.data:
            
            if avg_cash is None:
                avg_cash = 0
            
            p.setBrush(pg.mkBrush('g'))
            p.drawRect(QtCore.QRectF(t - w, 0, w * 2, avg_cash))
                
        p.end()

    def paint(self, p, *args):
        p.drawPicture(0, 0, self.picture)

    def boundingRect(self):
        ## boundingRect _must_ indicate the entire area that will be drawn on
        ## or else we will get artifacts and possibly crashing.
        ## (in this case, QPicture does all the work of computing the bouning rect for us)
        return QtCore.QRectF(self.picture.boundingRect())
    
    
    
"""   
data = [  ## fields are (time, open, close, min, max).
    (1., 10, 13, 5, 15),
    (2., 13, 17, 9, 20),
    (3., 17, 14, 11, 23),
    (4., 14, 15, 5, 19),
    (5., 15, 9, 8, 22),
    (6., 9, 15, 8, 16),
] 
"""    