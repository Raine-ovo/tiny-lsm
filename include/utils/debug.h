// include/utils/debug.h
#ifndef UTILS_DEBUG_H  // 头文件保护
#define UTILS_DEBUG_H

#include <iostream>
#include <utility>  // 用于 std::forward

// 全局调试开关
inline bool Debug = false;

inline void debug() {
    if (Debug) {
        std::cout << std::endl;
    }
}

template <typename First, typename... Rest>
inline void debug(First&& first, Rest&&... rest) {
    if (Debug) {
        std::cout << std::forward<First>(first);
        
        if constexpr (sizeof...(rest) > 0) {
            std::cout << " ";
            debug(std::forward<Rest>(rest)...);
        } else {
            std::cout << std::endl;
        }
    }
}

#endif  // UTILS_DEBUG_H