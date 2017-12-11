package com.pythian

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
import com.google.gson.JsonObject
import com.google.api.services.bigquery.model.TableRow

/* Simple singleton object wrapping memoized function */
object Dynamic {
  private val toolbox = currentMirror.mkToolBox()
  private val dynamic = new Memoize(10)((code: String) ⇒ {
    val tree = toolbox.parse(code)
    toolbox.eval(tree).asInstanceOf[JsonObject ⇒ TableRow]
  })
  def apply(code: String) = dynamic(code)

  implicit class RichString(val code: String) extends AnyVal {
    def evalFor(arg: JsonObject): TableRow = Dynamic(code)(arg)
  }
}
