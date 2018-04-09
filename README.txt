APPROACH FOR PEER NODE
Note :- As stated in the problem, this program works for small files (512 bytes to 1024 bytes)
1. We first run the root peer with the host and the port number. 
2. The root peer initializes the file list in that peer node(Can be empty initially).
3. The node calculates its nodeID by hashing the IP and the port entered and obtains a value in the range of 0 to 1024.
4.  The root peer acts as a server and waits for a connection from other peers as well as the client.
5. When a new peer joins, the root checks if the root is alone or not. If it is alone it forms a ring with the incoming new node.
6. If it is not alone, it calculates the positon of the new node in that ring and returns the info to that node.
7. The root sends update instructions to all the nodes which get affected by the new node.
8. The nodes then form a ring and listen for instructions.
9. The nodes also check for successor crashes in parallel using threads.
10. If a node crashes, the ring repairs itself by connecting the current node to the next successor and vice-versa.
11. The update instructions are sent throughout the ring.
12. With each entry and exit, the nodes whose successor and predecessor are updated prints them.
13. As multiple functions can update predecessor and successor of a particular node, that info can be printed multiple times.
14. The last printed successor and predecessor node represents the final nodes. 
15. When the client contacts the root or other nodes for file storage or look up, the perform the necessary actions.
16. In case of a file storage, the node updates the file list it maintains and also stores the list of files in an index file for future reference
17. If a file belongs to a new node, the node performs load balancing and sends the file to the desired node. It deletes the file from its entries, thus balancing the node.

APPROACH FOR CLIENT
1. The client first tries to connect to the root. If the connection is successful, a menu is displayed to the user.
2. The menu contains 4 options:-
    a. File storage
    b. File Look up Iteratively
    c. File Look up Recursively
    d. exit
3. When the user chooses file Storage, the client sends the file details (object ID and file name) to the root.
4. The root returns the node where the file belongs and the client sends the file to that node.
5. When the user chooses File Lookup Iteratively, the client sends the details of the file to the root.
6. The root tells if the file is in the system or not. If it is in the system, the root returns the node at which the file is stored.
7. The client contacts the returned node and retrives the file.
8. When the user chooses File Lookup Recursively, the client sends the details of the file to the root.
9. If the file is located at the root, it informs the client. Else, it tells the client that it does not have the file.
10. The same procedure is repeated for subsequent nodes.
11. If the file is in the system, the client contacts the node at which it is located and retrives the file.
12. If not, the root tells the client that the entered file is not in the system.
13. The exit option exits the client program.


TESTING
1. While Testing I made sure to try all combinations of node ID entering, that is, different permutations of node ID joining combinations.
2. While testing peer system, I made sure to test thoroughly that what happens when different nodes crash.
3. Joining the node at the end of the ring(before the smallest or greater than largest) was tested thoroughly.
4. With the client, I tested the programs with different files(texts images etc)
5. I tested in particular that if a client enters a name of the file that is not in the system, how the does the peer system responds.
6. Also, at last but not the least, i made sure that with every node join, if there is a need for load balancing, the system balances the node automatically.

HOW TO RUN THE PROGRAM
Open Command Line
Type 'make build' and execute that
Once the makefile is executed, run according to the command given in the project question.