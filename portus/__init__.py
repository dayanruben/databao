import logging
from langchain_core.language_models.chat_models import BaseChatModel
from typing import Union
from langchain.chat_models import init_chat_model

from portus.session import Session, SessionImpl

logger = logging.getLogger(__name__)
# Attach a NullHandler so importing apps without logging config don’t get warnings.
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


def create_session(llm: Union[str, BaseChatModel]) -> Session:
    return SessionImpl(llm if isinstance(llm, BaseChatModel) else init_chat_model(llm))
