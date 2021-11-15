// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <functional>
#include <type_traits>

namespace base {

// See
// http://stackoverflow.com/questions/32007938/how-to-access-a-possibly-unexisting-type-alias-in-c11
// and http://stackoverflow.com/questions/27687389/how-does-void-t-work
// and
template <typename... Ts> struct voider { using type = void; };

template <typename... Ts> using void_t = typename voider<Ts...>::type;

#if __cpp_lib_invoke >= 201411
using std::invoke;
using std::is_invocable;
using std::is_invocable_r;
#else

// Bringing is_invokable from c++17.
template <typename F, typename... Args>
struct is_invocable : std::is_constructible<std::function<void(Args...)>,
                                            std::reference_wrapper<std::remove_reference_t<F>>> {};

template <typename R, typename F, typename... Args>
struct is_invocable_r : std::is_constructible<std::function<R(Args...)>,
                                              std::reference_wrapper<std::remove_reference_t<F>>> {
};

template <typename F, typename... Args>
constexpr auto invoke(F&& f, Args&&... args) noexcept(
    noexcept(static_cast<F&&>(f)(static_cast<Args&&>(args)...)))
    -> decltype(static_cast<F&&>(f)(static_cast<Args&&>(args)...)) {
  return static_cast<F&&>(f)(static_cast<Args&&>(args)...);
}

#endif

// based on https://functionalcpp.wordpress.com/2013/08/05/function-traits/
template <typename F> struct DecayedTupleFromParams;

/*template <typename C, typename R, typename Arg> struct DecayedTupleFromParams<R(C::*)(Arg)> {
  typedef typename std::decay<Arg>::type type;
};*/

template <typename C, typename R, typename... Args>
struct DecayedTupleFromParams<R (C::*)(Args...)> {
  typedef std::tuple<typename std::decay<Args>::type...> type;
};

template <typename C, typename R, typename... Args>
struct DecayedTupleFromParams<R (C::*)(Args...) const> {
  typedef std::tuple<typename std::decay<Args>::type...> type;
};

template <typename C>
struct DecayedTupleFromParams : public DecayedTupleFromParams<decltype(&C::operator())> {};



template <typename RType, typename... Args> struct FunctionSig {
  //! arity is the number of arguments.
  static constexpr size_t arity = sizeof...(Args);

  using result_type = RType;

  //! the tuple of arguments
  using args_tuple = std::tuple<Args...>;

  //! the tuple of arguments: with remove_cv and remove_reference applied.
  using args_tuple_plain =
      std::tuple<typename std::remove_cv<typename std::remove_reference<Args>::type>::type...>;

  //! the i-th argument is equivalent to the i-th tuple element of a tuple
  //! composed of those arguments.
  template <size_t i> using arg = typename std::tuple_element<i, args_tuple>::type;

  //! return i-th argument reduced to plain type: remove_cv and
  //! remove_reference.
  template <size_t i>
  using arg_plain = typename std::remove_cv<typename std::remove_reference<arg<i>>::type>::type;
};

// Based on
// https://stackoverflow.com/questions/7943525/
template <typename T> struct function_traits : public function_traits<decltype(&T::operator())> {};

//! specialize for pointers to const member function
template <typename C, typename RType, typename... Args>
struct function_traits<RType (C::*)(Args...) const> : public FunctionSig<RType, Args...> {
  using is_const = std::true_type;
  using ClassType = C;
  using is_member = std::true_type;
};

//! specialize for pointers to mutable member function
template <typename C, typename RType, typename... Args>
struct function_traits<RType (C::*)(Args...)> : public FunctionSig<RType, Args...> {
  using is_const = std::false_type;
  using ClassType = C;
  using is_member = std::true_type;
};

//! specialize for function pointers
template <typename RType, typename... Args>
struct function_traits<RType(Args...)> : public FunctionSig<RType, Args...> {};

template <typename R, typename... Args>
struct function_traits<R (*)(Args...)> : public function_traits<R(Args...)> {};

template <typename T> struct function_traits<T&> : function_traits<T> {};

template <typename T> struct function_traits<T&&> : function_traits<T> {};

}  // namespace base

// Right now these macros are no-ops, and mostly just document the fact
// these types are PODs, for human use.  They may be made more contentful
// later.  The typedef is just to make it legal to put a semicolon after
// these macros.
// #define DECLARE_POD(TypeName) typedef int Dummy_Type_For_DECLARE_POD
#define PROPAGATE_POD_FROM_TEMPLATE_ARGUMENT(TemplateName) \
  typedef int Dummy_Type_For_PROPAGATE_POD_FROM_TEMPLATE_ARGUMENT

#if 1
#define GENERATE_TYPE_MEMBER_WITH_DEFAULT(Type, member, def_type)                \
  template <typename T, typename = void> struct Type { using type = def_type; }; \
                                                                                 \
  template <typename T> struct Type<T, ::base::void_t<typename T::member>> {     \
    using type = typename T::member;                                             \
  }

// specialized as has_member< T , void > or discarded (sfinae)
#define DEFINE_HAS_MEMBER(name, member)                                  \
  template <typename, typename = void> struct name : std::false_type {}; \
  template <typename T> struct name<T, ::base::void_t<decltype(T::member)>> : std::true_type {}

// Use it like this:
// DEFINE_HAS_SIGNATURE(has_foo, T::foo, void (*)(void));
//
#define DEFINE_HAS_SIGNATURE(TraitsName, funcName, signature)               \
  template <typename U> class TraitsName {                                  \
    template <typename T, T> struct helper;                                 \
    template <typename T> static char check(helper<signature, &funcName>*); \
    template <typename T> static long check(...);                           \
                                                                            \
   public:                                                                  \
    static constexpr bool value = sizeof(check<U>(0)) == sizeof(char);      \
    using type = std::integral_constant<bool, value>;                       \
  }

#define DEFINE_GET_FUNCTION_TRAIT(Name, FuncName, Signature)                            \
  template <typename T> class Name {                                                    \
    template <typename U, U> struct helper;                                             \
    template <typename U> static Signature Internal(helper<Signature, &U::FuncName>*) { \
      return &U::FuncName;                                                              \
    }                                                                                   \
    template <typename U> static Signature Internal(...) { return nullptr; }            \
                                                                                        \
   public:                                                                              \
    static Signature Get() { return Internal<T>(0); }                                   \
  }

#endif
