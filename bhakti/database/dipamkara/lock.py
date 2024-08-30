import asyncio

vector_modify_lock = asyncio.Lock()
inverted_index_modify_lock = asyncio.Lock()
document_modify_lock = asyncio.Lock()
auto_increment_lock = asyncio.Lock()
