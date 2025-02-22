
# Parallel File Streaming with TCP 

## Concept diagram
<div align="center">
  <img src="./demo.gif" alt="Architecture Diagrams" width =900 height =500>
</div>

## overview
This project implements a parallel file streaming in go using multiple tcp connnection between client and server , sender reads a large file in chunks and sends them in parallel to client , __synchrouszation is achieved by used mathematical mod which also acts like load balenced of tcp connections__ receiver side or client side 


## synchrozing chunks/packets between client and server 

file is read is chunck and each chunk will be sent to connection with  equation 

$$
  connection id = (chunk id ) mod total connections 
$$