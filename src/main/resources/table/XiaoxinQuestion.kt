package me.demo.flink.table

import cn.hutool.json.JSONUtil
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.Schema
import org.apache.flink.types.Row
import org.apache.flink.types.RowKind
import java.util.Date

class XiaoxinQuestion {

    companion object {
        // changelog stream Schema
        val schema: Schema = Schema.newBuilder()
                .column("id", DataTypes.STRING().notNull())
                .column("robot_id", DataTypes.STRING())
                .column("standard_question_id", DataTypes.STRING())
                .column("hf_id", DataTypes.STRING())
                .column("user_question", DataTypes.STRING())
                .column("question_type", DataTypes.STRING())
                .column("menu_id", DataTypes.STRING())
                .column("menu_retrieve", DataTypes.STRING())
                .column("is_standard_question", DataTypes.BOOLEAN())
                .primaryKey("id")
                .build()

        // data stream TypeInformation
        val typeInfo: TypeInformation<Row> = Types.ROW_NAMED(
                schema.columns.map { it.name }.toTypedArray(),
                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.BOOLEAN)
    }

    constructor()


    // 无匹配问
    constructor(question: NoMatchQuestion) {
        // 语料库-业务问字段
        robot_id = question.robot_id
        hf_id = question.id
        question_type = PreModelQuestionTypeEnum.NO_MATCH_QUESTION.code
        user_question = question.question
        id = buildId()
    }

    //组合id
    var id: String? = null

    var robot_id: String? = null
    var standard_question_id: String? = null
    var hf_id: String? = null

    var user_question: String? = null
    var question_type: String? = null
    var menu_id: String? = null
    var menu_retrieve: String? = null
    var is_standard_question: Boolean = false

    // 数据库自动生成
    var create_time: Date? = null
    var update_time: Date? = null

    private fun buildId(): String {
        return "$robot_id#${standard_question_id ?: "NULL"}#${hf_id ?: "NULL"}"
    }

    fun toRow(kind: RowKind): Row {
        val bean = JSONUtil.toBean(JSONUtil.toJsonStr(this), Map::class.java)
        val values = schema.columns.map { bean[it.name] }.toTypedArray()
        return Row.ofKind(kind, *values)
    }

    override fun toString(): String {
        return "XiaoxinQuestion(id=$id, robot_id=$robot_id, standard_question_id=$standard_question_id, hf_id=$hf_id, user_question=$user_question, question_type=$question_type, menu_id=$menu_id, menu_retrieve=$menu_retrieve, is_standard_question=$is_standard_question, create_time=$create_time, update_time=$update_time)"
    }


}
