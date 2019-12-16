import base64
import hmac
import logging

from DSpace import objects
from DSpace.context import RequestContext

log = logging.getLogger(__name__)

DEFAULT_KEY = 'JD98Dskw!23njQndW9D'
VERIFY_KEY = 'T2Stor'


class UserToken(object):
    def __init__(self):
        self.key = DEFAULT_KEY
        self.verify_key = VERIFY_KEY

    def get_context(self, user_id):
        ctxt = RequestContext(user_id=user_id, is_admin=False)
        return ctxt

    def generate_user_key(self, user_id):
        ctxt = self.get_context(user_id)
        user = objects.User.get_by_id(ctxt, user_id)
        if user:
            key = self.verify_key + '==' + str(user_id)
            return key

    def generate_token(self, user_id):
        user_key = self.generate_user_key(user_id)
        ts_byte = user_key.encode("utf-8")
        sha1_tshexstr = hmac.new(
            self.key.encode("utf-8"), ts_byte, 'sha1').hexdigest()
        token = user_key + ':' + sha1_tshexstr
        b64_token = base64.urlsafe_b64encode(token.encode("utf-8"))
        return b64_token.decode("utf-8")

    def certify_token(self, token):
        try:
            token_str = base64.urlsafe_b64decode(token).decode('utf-8')
        except Exception as e:
            log.exception('certify_token, base64 decode error:%s', e)
            return False, None
        token_list = token_str.split(':')
        if len(token_list) != 2:
            log.error('certify_token, len error, len(token_list) is two')
            return False, None
        ts_str = token_list[0]
        user_info = ts_str.split('==')
        verify_key = user_info[0]
        user_id = user_info[1]
        if verify_key != self.verify_key:
            log.error('certify_token, verify_key error')
            return False, None
        ctxt = self.get_context(user_id)
        user = objects.User.get_by_id(ctxt, user_id)
        if not user:
            log.error('certify_token, not exise user:%s', user_id)
            return False, None
        known_sha1_tsstr = token_list[1]
        sha1 = hmac.new(
            self.key.encode("utf-8"), ts_str.encode('utf-8'), 'sha1')
        calc_sha1_tsstr = sha1.hexdigest()
        if calc_sha1_tsstr != known_sha1_tsstr:
            log.error('certify_token, decode token error')
            return False, None
        return True, user_id


if __name__ == '__main__':
    user_token = UserToken()
    # user_id: 1
    token = user_token.generate_token(1)
    print(token)
    # print(token)
    print(user_token.certify_token(token))
