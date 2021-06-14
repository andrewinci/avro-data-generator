package com.github.andrewinci

import munit.FunSuite

class HelloSpec extends FunSuite {
  test("dummy") {
    assertEquals(Hello.greeting, "hello")
  }
}
