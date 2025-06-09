# sharding.py
import json
import pickledb


class ShardedDatabase:
    # Read books from JSON file

    def __init__(self):
        self.num_nodes = 10
        self.nodes = {i: pickledb.load(f"database_node_{i}.db", False) for i in range(0, 10)}
        self.store_books()

    def hash_key(self, book):
        # For this example, we determine the node based on the first letter of the key

        if not book[0].isalpha():
            return 9
        first_letter = book[0].upper()
        if 'A' <= first_letter <= 'C':
            return 0
        elif 'D' <= first_letter <= 'F':
            return 1
        elif 'G' <= first_letter <= 'I':
            return 2
        elif 'J' <= first_letter <= 'L':
            return 3
        elif 'M' <= first_letter <= 'O':
            return 4
        elif 'P' <= first_letter <= 'R':
            return 5
        elif 'S' <= first_letter <= 'U':
            return 6
        elif 'V' <= first_letter <= 'X':
            return 7
        elif 'Y' <= first_letter <= 'Z':
            return 8
        else:
            return 0

    def store_books(self):
        for book in books:
            # Map study courses to hash-modulo keys
            node_index = self.hash_key(book)
            self.nodes[node_index].set(book, node_index)
            self.nodes[node_index].dump()

    def check_if_book_exists(self, book_name):
        node_index = self.hash_key(book_name)
        if self.nodes[node_index].exists(book_name):
            print("The book ", book_name, "is stored in database node ", node_index)
        else:
            print("The book ", book_name, "is not found in the database.")

    ERROR_MESSAGE_INVALID_NODE = "The following Node doesn't exist."
    ERROR_MESSAGE_ALREADY_EMPTIED_NODE = "Node {} had already been emptied."
    INFO_MESSAGE_EMPTIED_NODE = "Node {} has been emptied."

    def empty_node(self, node_index):
        if 0 <= node_index <= 9:
            # Clear all entries in the specified node
            all_keys = list(self.nodes[node_index].getall())

            if len(all_keys) == 0:
                return self.ERROR_MESSAGE_ALREADY_EMPTIED_NODE.format(node_index)

            for key in all_keys:
                self.nodes[node_index].rem(key)

            # Save the changes
            self.nodes[node_index].dump()
            return self.INFO_MESSAGE_EMPTIED_NODE.format(node_index)

        else:
            return self.ERROR_MESSAGE_INVALID_NODE

    def empty_nodes(self, nodes_to_empty):
        messages = []
        for node_index in nodes_to_empty:
            messages.append(self.empty_node(node_index))
        return messages

    # TODO 1: implement this method as stated in the exercise description
    def doesDBContainKey(self, key: str):  # 比如 key = "book1"
        shard = self.hash_key(key)  # 计算book1在哪个分片

        return self.nodes[shard].exists(key)  # 找到分片并查看该分片是否有key
    
    # TODO 2: implement this method as stated in the exercise description
    def doesDBContainKeys(self, keys: list):
        for key in keys:
            if not self.doesDBContainKey(key):
                return False
        return True

    ERROR_MESSAGE_INVALID_DELTA = "The values still in the database are not what they should be"
    replicate_nodes = None

    # TODO 3: implement this method as stated in the exercise description
    #  This method should take a list of node indices (nodes_to_empty).
    #  First you store the values previously stored in the database, distinguishing between whether they are supposed to be deleted or not

    #  Second you kill the respective nodes from the db (Use the provided methods empty_nodes()/empty_node())

    #  Third, if at least one key was deleted, you check that the database no longer contains the elements of the killed nodes but still contains the elements of those that have not been killed.
    #  If this does not hold, raise an exception with the given message (Use doesDBContainKeys()!)

    #  Finally return the two lists of values (one which contains the elements still available, one which contains the elements deleted through killing the nodes)
    #  As you see, sharding leads to an increase in availability,
    #  simply because killing some nodes does not necessarily lead to all data being unavailable.
    #  Instead, a certain amount of data is still available. To ensure a very low chance of data loss,
    #  one would store each key in several nodes, so it is unlikely all nodes die at the same time.

    def empty_nodes_check_remaining(self, nodes_to_empty=None):
        list_remain = []
        list_kill = []

        for key in self.nodes.keys():
            if key == self.nodes[nodes_to_empty]:
                list_kill.append(key)
            else:
                list_remain.append(key)

        self.empty_nodes(nodes_to_empty)

        if self.doesDBContainKeys(list_kill):
            raise Exception("Wrong")

        return list_remain, list_kill
    
    # TODO 4: implement this method as stated in the exercise description
    def create_replicates(self):
        return self.nodes.copy()
    
    # TODO 5: implement this method as stated in the exercise description
    def recover_node(self, node_index):
        rep = self.create_replicates()
        if node_index not in rep:
            raise ValueError("Replica not found.")
        #特定index的主分片丢失，想用副本恢复主分片
        to_recover_node = rep[node_index]  # 找到副本中index的位置的node
        self.nodes[node_index] = to_recover_node  # 从副本恢复到主分片
        return to_recover_node  # return the recovered node.

    
    # TODO 6: implement this method as stated in the exercise description
    def recover_nodes(self,nodes_to_recover):
        result = []
        for n in nodes_to_recover:
            result.append(self.recover_node(n))

        return result



with open('books.json', 'r') as json_file:
    books_data = json.load(json_file)

# Extract books list from JSON data
books = books_data['books']

sharded_db = ShardedDatabase()

sharded_db.create_replicates()

nodes_to_be_emptied = [3, 4]
try:
    still_available, deleted = sharded_db.empty_nodes_check_remaining(nodes_to_be_emptied)
    print("Still available ", still_available)
    print("Deleted ", deleted)
except:
    print("empty_nodes_check_remaining()-method not implemented!")

sharded_db.recover_nodes(nodes_to_be_emptied)

for node_index in nodes_to_be_emptied:
    original_contents = list(sharded_db.nodes[node_index].getall())
    print(original_contents)
