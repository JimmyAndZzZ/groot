package com.jimmy.groot.engine.core.metadata;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.groot.engine.base.Index;
import com.jimmy.groot.engine.core.other.ConditionPart;
import com.jimmy.groot.engine.core.index.PrimaryKey;
import com.jimmy.groot.engine.exception.EngineException;
import com.jimmy.groot.sql.core.Condition;
import com.jimmy.groot.sql.core.ConditionGroup;
import com.jimmy.groot.sql.enums.ConditionEnum;
import com.jimmy.groot.sql.enums.ConditionTypeEnum;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class Table implements Serializable {

    private static final String SOURCE_PARAM_KEY = "source";

    private static final String TARGET_PARAM_KEY = "target";

    private String schema;

    private String tableName;

    private PrimaryKey primaryKey;

    private List<Column> columns = Lists.newArrayList();

    private List<Index> indices = Lists.newArrayList();


    

    /**
     * 获取表达式
     */
    private List<ConditionPart> getConditionExp(List<ConditionGroup> conditionGroups) {
        int i = 0;
        List<ConditionPart> parts = Lists.newArrayList();
        //遍历关联关系
        for (ConditionGroup conditionGroup : conditionGroups) {
            List<Condition> groupConditions = conditionGroup.getConditions();

            if (CollUtil.isNotEmpty(groupConditions)) {
                ConditionPart part = new ConditionPart();

                StringBuilder childCondition = new StringBuilder();
                for (Condition condition : groupConditions) {
                    if (StrUtil.isNotBlank(childCondition)) {
                        childCondition.append(ConditionTypeEnum.AND.getExpression());
                    }

                    part.getMayNeedIndexFields().add(condition.getFieldName());
                    childCondition.append(this.getExpCondition(condition.getFieldName(), condition.getFieldValue(), condition.getConditionEnum(), part.getConditionArgument(), i++));
                }

                part.setExpression(childCondition.toString());
                parts.add(part);
            }
        }

        return parts;
    }

    /**
     * 构建表达式
     *
     * @param name
     * @param fieldValue
     * @param conditionEnum
     * @param target
     * @param i
     * @return
     */
    private String getExpCondition(String name, Object fieldValue, ConditionEnum conditionEnum, Map<String, Object> target, int i) {
        String keyName = name + "$" + i;
        StringBuilder conditionExp = new StringBuilder();
        conditionExp.append(SOURCE_PARAM_KEY).append(".").append(name);

        switch (conditionEnum) {
            case EQ:
                conditionExp.append("==").append(TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case GT:
                conditionExp.append("> ").append(TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case GE:
                conditionExp.append(">=").append(TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case LE:
                conditionExp.append("<=").append(TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case LT:
                conditionExp.append("< ").append(TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case IN:
                conditionExp.setLength(0);
                conditionExp.append(" in (").append(SOURCE_PARAM_KEY).append(".").append(name).append(",").append(TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("in 操作需要使用集合类参数");
                }

                target.put(keyName, fieldValue);
                break;
            case NOT_IN:
                conditionExp.setLength(0);
                conditionExp.append(" notIn (").append(SOURCE_PARAM_KEY).append(".").append(name).append(",").append(TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("not in 操作需要使用集合类参数");
                }

                target.put(keyName, fieldValue);
                break;
            case NULL:
                conditionExp.append("==nil");
                break;
            case NOT_NULL:
                conditionExp.append("!=nil");
                break;
            case NE:
                conditionExp.append("!=").append(TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, fieldValue);
                break;
            case NOT_LIKE:
                conditionExp.setLength(0);
                conditionExp.append("!string.contains(").append(SOURCE_PARAM_KEY).append(".").append(name).append(",").append(TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                target.put(keyName, this.likeValueHandler(fieldValue));
                break;
            case LIKE:
                conditionExp.setLength(0);
                conditionExp.append("string.contains(").append(SOURCE_PARAM_KEY).append(".").append(name).append(",").append(TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                target.put(keyName, this.likeValueHandler(fieldValue));
                break;
            default:
                throw new EngineException("不支持查询条件");
        }

        return conditionExp.toString();
    }

    /**
     * like字段处理，处理掉百分号
     *
     * @param value
     * @return
     */
    private String likeValueHandler(Object value) {
        if (value == null) {
            throw new EngineException("模糊查询值为空");
        }

        String like = value.toString().trim();
        if (StrUtil.startWith(like, "%")) {
            like = StrUtil.sub(like, 1, like.length());
        }

        if (StrUtil.endWith(like, "%")) {
            like = StrUtil.sub(like, 0, like.length() - 1);
        }

        if (StrUtil.isEmpty(like)) {
            throw new EngineException("模糊查询值为空");
        }

        return like;
    }

}
