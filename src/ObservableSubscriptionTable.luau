
--[=[
	This allows the storage of subscriptions for keys, such that something
	can subscribe onto a key, and events can be invoked onto keys.
	@class ObservableSubscriptionTable
]=]

const Observable = require('./Observable')
-- selene: allow(unused_variable)
const Subscription = require('./Subscription')

const ObservableSubscriptionTable = {}
ObservableSubscriptionTable.ClassName = 'ObservableSubscriptionTable'
ObservableSubscriptionTable.__index = ObservableSubscriptionTable

export type ObservableSubscriptionTable<T...> = typeof(setmetatable(
	{} :: {
		_subMap: { [any]: { Subscription.Subscription<T...> } },
	},
	{} :: typeof({ __index = ObservableSubscriptionTable })
))

@checked
function ObservableSubscriptionTable.new<T...>(): ObservableSubscriptionTable<T...>
	const self: ObservableSubscriptionTable<T...> = setmetatable({} :: any, ObservableSubscriptionTable)
	self._subMap = {}

	return self
end

@checked
function ObservableSubscriptionTable.Fire<T...>(self: ObservableSubscriptionTable<T...>, key: any, ...): ()
	assert(key ~= nil, 'Bad key')

	const subs = self._subMap[key]
	if not subs then
		return
	end

	for _, sub: any in table.clone(subs) do
		if sub:IsPending() then
			task.spawn(sub.Fire, sub, ...)
		end
	end
end

@checked
function ObservableSubscriptionTable.GetRawSubscriptionMap<T...>(
	self: ObservableSubscriptionTable<T...>
): { [any]: { Subscription.Subscription<T...> } }
	return self._subMap
end

@checked
function ObservableSubscriptionTable.HasSubscriptions<T...>(self: ObservableSubscriptionTable<T...>, key: any): boolean
	return self._subMap[key] ~= nil
end

@checked
function ObservableSubscriptionTable.HasAnySubscriptions<T...>(self: ObservableSubscriptionTable<T...>): boolean
	return next(self._subMap) ~= nil
end

@checked
function ObservableSubscriptionTable.Complete<T...>(self: ObservableSubscriptionTable<T...>, key: any): ()
	const subs = self._subMap[key]
	if not subs then
		return
	end

	const subsToComplete = table.clone(subs)
	self._subMap[key] = nil

	for _, sub: any in subsToComplete do
		if sub:IsPending() then
			task.spawn(sub.Complete, sub)
		end
	end
end

@checked
function ObservableSubscriptionTable.Fail<T...>(self: ObservableSubscriptionTable<T...>, key: any): ()
	const subs = self._subMap[key]
	if not subs then
		return
	end

	const subsToFail = table.clone(subs)
	self._subMap[key] = nil

	for _, sub: any in subsToFail do
		if sub:IsPending() then
			task.spawn(sub.Fail, sub)
		end
	end
end

export type RetrieveInitialValue<T...> = (sub: Subscription.Subscription<T...>) -> ()

@checked
function ObservableSubscriptionTable.Observe<T...>(
	self: ObservableSubscriptionTable<T...>,
	key: any,
	retrieveInitialValue: RetrieveInitialValue<T...>?
): Observable.Observable<T...>
	assert(key ~= nil, 'Bad key')

	return Observable.new(function(sub)
		if not self._subMap[key] then
			self._subMap[key] = { sub }
		else
			table.insert(self._subMap[key], sub)
		end

		if retrieveInitialValue then
			retrieveInitialValue(sub)
		end

		return function()
			const current = self._subMap[key]
			if not current then
				return
			end

			const index = table.find(current, sub)
			if not index then
				return
			end

			table.remove(current, index)
			if #current == 0 then
				self._subMap[key] = nil
			end

			if sub:IsPending() then
				task.spawn(sub.Complete, sub)
			end
		end
	end)
end

@checked
function ObservableSubscriptionTable.Destroy<T...>(self: ObservableSubscriptionTable<T...>): ()
	while next(self._subMap) do
		const key, list = next(self._subMap)
		assert(key, 'Key should not be nil')

		self._subMap[key] = nil

		for _, sub: any in list do
			if sub:IsPending() then
				task.spawn(sub.Complete, sub)
			end
		end
	end
end

return ObservableSubscriptionTable
