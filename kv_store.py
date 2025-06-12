# kv_store.py
import json
import random
import string
import os


def random_key_value(key=None, l=8):
    """
    Generate a random key-value pair.

    Args:
        key (str): Key to use in the tuple.
        l (int): Length of value to generate.

    Returns:
        tuple: A tuple containing a random key and value.
    """
    if key is None:
        key = ''.join(random.choices(string.ascii_lowercase, k=5))
    value = ''.join(random.choices(string.digits, k=l))
    return key, value


def generate_random_operations(n):
    """
    Generate a list of 'N' random operations.

    Args:
        n (int): Number of random operations to generate.

    Returns:
        list: List of randomly generated operations.
    """
    operations = []
    for _ in range(n):
        key, value = random_key_value()
        action = random.choice(["set", "delete", "add", "subtract", "multiply", "divide"])
        if action == "delete":
            operations.append({"action": "delete", "key": key})
        else:
            operations.append({"action": action, "key": key, "value": value})
    return operations


def log_and_apply_operations(operation_list, store, log_file):
    """
    Log the operation and apply it to the store.

    Args:
        operation_list (list of dict): List of operations to log and apply.
        store (dict): The key-value store to apply the operation to.
        log_file (str): The file to log the operation.

    Returns:
        None
    """
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    with open(log_file, 'a') as file:
        file.write(json.dumps(operation_list) + "\n")

    for operation in operation_list:
        apply_operation(operation, store)


def apply_log(file_name, store):
    """
    Apply the operations from a log file to the store.

    Args:
        file_name (str): The name of the log file.
        store (dict): The key-value store to apply the operations to.

    Returns:
        None
    """
    with open(file_name, 'r') as file:
        for line in file:
            operations = json.loads(line)
            for op in operations:
                apply_operation(op, store)


def convert_string_to_number(num):
    """
    Converts the string representation of a number back to a float.

    Args:
        num (str): The string to convert

    Returns:
        float: the converted number
    """
    return float(num)


def apply_operation(operation, store):
    """
    Applies one operation to the store.

    Args:
        operation (dict): The operation to apply to the store.
        store (dict): The key-value store to apply the operation to.

    Returns:
        None
    """
    action = operation["action"]
    key = operation["key"]

    # TODO Apply operations to store
    # Remember: We are only considering numerical values, but have more actions
    # Hint: use the convert_string_to_number() function
    # The values are still stored as strings in the kv store
    # Hint: After calculation with numerical type, convert into str
    # ["set", "delete", "add", "subtract", "multiply", "divide"]
    # When applying mathematical operations on a non-existent key, initialize it with value 0

    # non-existent key handling here
    # TODO
    value = operation.get("value", "0")  # "value" 代表key

    # apply: convert_string_to_number()
    OpValue_in_number = convert_string_to_number(value)
    StoreValue_in_number = convert_string_to_number(store.get(key, "0"))

    # operation handling here
    if action == "set":
        store[key] = str(convert_string_to_number(operation["value"]))
    elif action == "delete":
        store.pop(key)
    # other actions here
    # TODO
    elif action == "add":
        store[key] = str(StoreValue_in_number + OpValue_in_number)
    elif action == "subtract":
        store[key] = str(StoreValue_in_number - OpValue_in_number)
    elif action == "multiply":
        store[key] = str(StoreValue_in_number * OpValue_in_number)
    elif action == "divide":
        if OpValue_in_number != 0:
            store[key] = str(StoreValue_in_number / OpValue_in_number)
        else:
            raise Exception("Division can not be zero")


def main(initial_kv_store, operation_list_list, undo_operation_list_list, redo_log_file, undo_log_file):
    """
    Perform the main process of logging, applying operations, generating undo logs,
    and comparing states.

    Args:
        initial_kv_store (dict): The initial key-value store.
        operation_list_list (list of list of dict): List of list of operations to perform.
        undo_operation_list_list (list of list of dict): List to store undo operations.
        redo_log_file (str): The file to log redo operations.
        undo_log_file (str): The file to log undo operations.

    Returns:
        tuple: A tuple containing the updated key-value store and the comparison store.
    """
    kv_store = initial_kv_store.copy()

    for operation_list in operation_list_list:
        # TODO Step 1: Log and apply operations
        # Hint: you should consider using redo_log_file with logging function.
        log_and_apply_operations(operation_list, kv_store, redo_log_file)

    comparison_kv_store = kv_store.copy()

    # Step 2: Generate and write Undo Log
    for operation_list in reversed(operation_list_list):  # Reverse the order for undo operations
        undo_operations_list = []
        for operation in reversed(operation_list):
            action = operation["action"]
            key = operation["key"]
            # TODO Undo the action of the operation
            # ["set", "delete", "add", "subtract", "multiply", "divide"]
            # Hint: Consider if the key existed in the initial store or not
            # Hint: Consider machine precision for division
            if action == "set" and key in initial_kv_store:
                undo_operations_list.append({"action": "delete", "key": key, "value" : initial_kv_store[key]})
            if action == "set" and key not in initial_kv_store:
                undo_operations_list.append({"action": "delete", "key": key})
            if action == "delete" and key in initial_kv_store:
                undo_operations_list.append({"action": "set", "key": key, "value": initial_kv_store[key]})
            if action in ("add", "subtract", "multiply", "divide") and key in initial_kv_store:
                undo_operations_list.append({"action": "set", "key": key, "value": initial_kv_store[key]})
            if action in ("add", "subtract", "multiply", "divide") and key not in initial_kv_store:
                undo_operations_list.append({"action": "delete", "key": key})
        undo_operation_list_list.append(undo_operations_list)

    # Write undo log.
    with open(undo_log_file, "w") as file:
        for operation_list in undo_operation_list_list:
            # TODO Step 3: Write undo log to corresponding log file.
            file.write(json.dumps(operation_list) + "\n")

    # TODO Step 4: Apply Undo Log
    # apply_log here
    apply_log(undo_log_file, kv_store)  # 这步要恢复到初始状态

    # TODO Step 5: Apply Redo Log
    # apply_log here
    apply_log(redo_log_file, kv_store)  # 这步是重新做，目的是要恢复到最后的状态

    # Step 6: Comparison of initial state and the state after the log files
    return kv_store, comparison_kv_store


if __name__ == "__main__":
    initial_store = {"test1": "100", "test2": "200"}

    operations = [generate_random_operations(3) for _ in range(2)]

    undo_ops = []
    redo_log = "redo.log"
    undo_log = "undo.log"

    final_store, comparison_store = main(
        initial_store,
        operations,
        undo_ops,
        redo_log,
        undo_log
    )

    print("Initial store:", initial_store)
    print("Final store:", final_store)
    print("Comparison store:", comparison_store)


