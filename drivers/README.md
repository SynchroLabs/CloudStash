# Cursor-based functions (directory listing, long polling)

Neither the file driver nor the Manta driver have any way of processing cursors that is better than
brute force traversal.  It is possible that with some tweaks to Manta, that there could be significant
optimizations.  It is also possible that an approach using metadata (that we update on write) could lead
to significant optimizations.

All drivers must implement:

    getObjectMetaData (get entry details for given file or folder)
    listDirectory (non-recursive, return all file and folder entries for a given parent)

The server is capable of using getObjectMetaData and listDirectory to handle all cursor-based processing
using brute force.  But the server will introspect the driver to see if it implements optimized cursor processing,
and if so, it will use those methods.  The optimized cursor processing functions are:

    listFolderUsingCursor - /files/list_folder + /files/list_folder/continue
    getLatestCursorItem - /files/list_folder/get_latest_cursor
    isAnyCursorItemNewer - /files/list_folder/longpoll

There could also be a fallback progression here.  For example, if a driver didn't implement isAnyCursorItemNewer,
but did implement getLatestCursorItem, /files/list_folder/longpoll could just use that to get the latest cursor
item and see if that one is newer.  Similarly, if a driver didn't implement getLatestCursorItem, but did implement
listFolderUsingCursor, /files/list_folder/get_latest_cursor could use listFoldersUsingCursor and use the last 
entry returned.  The assumption here is that any implemented method is a significant improvement over brute
force traversal, so it should be preferred.

# Multipart upload

Multipart upload (using our own mechanism) is supported without requiring any specific support from the driver. 
If a driver wants to implement its own multipart upload, it may do so by providing the following functions:

    startMultipartUpload
    multipartUpoad
    finishMultipartUpload

Note: We might want to provide a way to provide private context data throughout the multipart upload operation
      in case the driver needs state.
