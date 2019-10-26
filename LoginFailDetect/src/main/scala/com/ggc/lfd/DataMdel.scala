package com.ggc.lfd

// 输入的登录事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 输出的报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, waringMsg: String)

