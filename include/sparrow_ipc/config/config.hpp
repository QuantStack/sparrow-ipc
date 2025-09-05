#pragma once

#if defined(_WIN32)
#    if defined(SPARROW_IPC_STATIC_LIB)
#        define SPARROW_IPC_API
#    elif defined(SPARROW_IPC_EXPORTS)
#        define SPARROW_IPC_API __declspec(dllexport)
#    else
#        define SPARROW_IPC_API __declspec(dllimport)
#    endif
#else
#    define SPARROW_IPC_API __attribute__((visibility("default")))
#endif
