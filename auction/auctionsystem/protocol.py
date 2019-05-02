from enum import Enum

class PROTOCOL():
    DELIMITER = ':::'

class MESSAGE(Enum):
    REGISTER = 'REGISTER'
    REGISTERED = 'REGISTERED'
    UNREGISTERED = 'UNREGISTERED'
    DEREGISTER = 'DEREGISTER'
    DEREGISTER_CONFIRM = 'DEREG-CONF'
    DEREGISTER_DENIED = 'DEREG-DENIED'
    OFFER = 'OFFER'
    OFFER_CONFIRM = 'OFFER-CONF'
    OFFER_DENIED = 'OFFER-DENIED'
    NEW_ITEM = 'NEW-ITEM'
    BID = 'BID'
    BID_OVER = 'BID-OVER'
    HIGHEST = 'HIGHEST'
    WIN = 'WIN'
    SOLD_TO = 'SOLDTO'
    NOT_SOLD = 'NOT-SOLD'



class REASON(Enum):

    VALID = (b'VALID', 'valid')

    ALREADY_REGISTERED = (b'ALRD_REGISTERED',
                          'Client name is already registered.')
    BAD_IP = (b'BAD-IP',
              'IP address is not valid or is damaged.')

    NOT_REGISTERED = (b'NOT_REGISTERED',
                      'Client name not found in the registration table, might not have been registered at all.')

    ITEM_OFFERED = (b'ITEM_OFFERED',
                    'An item is currently being offered for auction.')

    ACTIVE_BID = (b'ACTIVE_BID',
                  'Client is currently leading with the highest bid for at least one item.')

    OFFER_LIMIT = (b'OFFER_LIMIT',
                   'Client cannot have more than 3 items offered for auction at a time.')

    NO_VALID_BIDS = (b'NO_VALID_BIDS',
                     'No valid bid exceeding this offered item\'s minimum purchase price was made on it.')

    def __init__(self, val, string):
        self.val = val
        self.str = string

    @staticmethod
    def dict():
        """
        Gets a dictionary of the enum values (better accessed from "AUCTION_CONSTS"
        :return: A dictionary of the value tuples, with the first elements being keys and the second being values.
        """
        reason_dict = {}
        for name, member in REASON.__members__.items():
            reason_dict[str(member.val)] = member.str
        return reason_dict

    @staticmethod
    def get_reason_str(reason):
        return REASON.dict().get(reason)



class AUCTION_CONSTS:
    OFFER_LIMIT = 3
    RESEND_LIMIT = 3
