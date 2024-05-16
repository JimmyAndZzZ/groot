package com.jimmy.groot.engine.core;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.jimmy.groot.engine.exception.EngineException;
import com.jimmy.groot.sql.core.Condition;
import com.jimmy.groot.sql.core.ConditionGroup;
import com.jimmy.groot.sql.core.QueryPlus;
import com.jimmy.groot.sql.core.Wrapper;
import com.jimmy.groot.engine.store.SegmentPool;
import com.jimmy.groot.engine.store.SegmentSerializer;
import com.jimmy.groot.sql.enums.ConditionEnum;
import com.jimmy.groot.sql.enums.ConditionTypeEnum;
import lombok.Data;

import java.util.*;


public class Partition extends Element {

    private static final String SOURCE_PARAM_KEY = "source";

    private static final String TARGET_PARAM_KEY = "target";

    private final Set<String> uniqueKeys;

    private final SegmentSerializer segmentSerializer;

    private final Map<String, Fragment> fragments = Maps.newHashMap();

    public Partition(Set<String> uniqueKeys, SegmentSerializer segmentSerializer) {
        this.uniqueKeys = uniqueKeys;
        this.segmentSerializer = segmentSerializer;
    }

    public List<Map<String, Object>> queryList(Wrapper wrapper) {
        Map<String, Object> conditionTarget = Maps.newHashMap();
        String conditionExp = this.getConditionExp(wrapper.getQueryPlus(), conditionTarget);

        Expression expression = AviatorEvaluator.compile(conditionExp);

        Map<String, Object> param = Maps.newHashMap();
        param.put(SOURCE_PARAM_KEY, "map");
        param.put(TARGET_PARAM_KEY, conditionTarget);
        Boolean flag = Convert.toBool(expression.execute(param), false);
        return null;

    }

    public void saveBatch(List<Map<String, Object>> docs) {
        for (Map<String, Object> doc : docs) {
            this.save(doc);
        }
    }

    public void remove(Map<String, Object> doc) {
        UniqueConstraint uniqueConstraint = this.getUniqueConstraint(doc);
        fragments.remove(uniqueConstraint.getCode());
    }

    public void save(Map<String, Object> doc) {
        UniqueConstraint uniqueConstraint = this.getUniqueConstraint(doc);

        Fragment fragment = new Fragment();
        fragment.setKey(uniqueConstraint.getUniqueData());
        fragment.setIndex(SegmentPool.getInstance().allocate(segmentSerializer.serialize(doc)));
        this.fragments.put(uniqueConstraint.getCode(), fragment);
    }

    /**
     * 获取唯一约束
     *
     * @param doc
     * @return
     */
    private UniqueConstraint getUniqueConstraint(Map<String, Object> doc) {
        Map<String, Object> uniqueData = Maps.newHashMap();

        for (String uniqueKey : uniqueKeys) {
            Object o = doc.get(uniqueKey);
            if (o == null) {
                throw new EngineException("主键为空,主键名:" + uniqueKey);
            }

            uniqueData.put(uniqueKey, o);
        }

        UniqueConstraint uniqueConstraint = new UniqueConstraint();
        uniqueConstraint.setUniqueData(uniqueData);
        uniqueConstraint.setCode(this.getCode(uniqueData));
        return uniqueConstraint;
    }

    /**
     * 获取表达式
     */
    private String getConditionExp(QueryPlus queryPlus, Map<String, Object> target) {
        int i = 0;
        List<Condition> conditions = queryPlus.getConditions();
        List<ConditionGroup> conditionGroups = queryPlus.getConditionGroups();
        //条件表达式模板
        StringBuilder conditionExp = new StringBuilder();
        //条件拼接
        if (CollUtil.isNotEmpty(conditions)) {
            StringBuilder conditionsExp = new StringBuilder();

            for (Condition condition : conditions) {
                if (StrUtil.isNotBlank(conditionsExp)) {
                    conditionsExp.append(condition.getConditionTypeEnum().getExpression());
                }

                conditionsExp.append(this.getExpCondition(condition.getFieldName(), condition.getFieldValue(), condition.getConditionEnum(), target, i++));
            }

            conditionExp.append("(").append(conditionExp).append(")");
        }
        //遍历关联关系
        for (ConditionGroup conditionGroup : conditionGroups) {
            List<Condition> groupConditions = conditionGroup.getConditions();
            ConditionTypeEnum conditionTypeEnum = conditionGroup.getConditionTypeEnum();

            if (CollUtil.isNotEmpty(groupConditions)) {
                StringBuilder childCondition = new StringBuilder();
                for (Condition condition : groupConditions) {
                    if (StrUtil.isNotBlank(childCondition)) {
                        childCondition.append(condition.getConditionTypeEnum().getExpression());
                    }

                    childCondition.append(this.getExpCondition(condition.getFieldName(), condition.getFieldValue(), condition.getConditionEnum(), target, i++));
                }

                if (StrUtil.isNotBlank(childCondition)) {
                    if (StrUtil.isNotBlank(conditionExp)) {
                        conditionExp.append(conditionTypeEnum.getExpression());
                    }

                    conditionExp.append("(").append(childCondition).append(")");
                }
            }
        }

        return conditionExp.toString();
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
