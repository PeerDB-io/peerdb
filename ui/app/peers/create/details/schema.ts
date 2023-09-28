import Joi from 'joi';

export const pgSchema = Joi.object({
    host:Joi.string().required(),
    port:Joi.number().integer().min(1).required(),
    database:Joi.string().min(1).max(100).required(),
    user:Joi.string().alphanum().min(1).max(64).required(),
    password:Joi.string().min(1).max(100).required(),
});