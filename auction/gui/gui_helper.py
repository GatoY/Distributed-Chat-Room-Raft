import tkinter as tk
import string
import random
from auctionsystem.protocol import *

SELECTION_EVENT_STR = '<<ListboxSelect>>'

# Auxiliary functions


def get_truncated_entry(entry, length):
    # Truncate the text
    text = entry.get()
    text = text[:length]
    # Ensure that the Protocol delimiter is not inputted into the text
    # Otherwise clients could manipulate bit data like SQL injections
    text = text.replace(PROTOCOL.DELIMITER, '')
    entry['text'] = text
    return text


def get_truncated_text(textobj, length):
    # Truncate the text
    text = textobj.get('1.0', 'end-1c')
    text = text[:length]
    # Ensure that the Protocol delimiter is not inputted into the text
    # Otherwise clients could manipulate bit data like SQL injections
    text = text.replace(PROTOCOL.DELIMITER, '')
    textobj.delete('1.0', 'end-1c')
    textobj.insert('1.0', text)
    return text


def gen_rand_str(length):
    rand_name = ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    return rand_name


def delete_listbox_item(item_to_remove, list_box):
    items = list_box.get(0, tk.END)
    item_index = items.index(item_to_remove)
    list_box.delete(item_index)


def item_in_listbox(item_to_check, list_box):
    return item_to_check in list_box.get(0, tk.END)


def get_formatted_display_text(response_str):
    words = response_str.split(' ')
    formatted_response = ''
    newline_counter = 0
    word_length = 8
    for word in words:
        if len(formatted_response) > 0:

            formatted_response += (' ' + word)

            if newline_counter == word_length - 1:
                formatted_response += '\n'
                newline_counter = -1  # Gets reset to zero at the last line of the for-loop
        else:
            formatted_response = word

        newline_counter += 1

    return formatted_response
