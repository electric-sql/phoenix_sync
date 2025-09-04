import {createCollection, localStorageCollectionOptions,} from '@tanstack/react-db'
import { ingestMutations } from './mutations'

import type { Value } from '@electric-sql/client'
import type { ElectricCollectionUtils } from '@tanstack/electric-db-collection'
import type {
  InsertMutationFn,
  UpdateMutationFn,
  DeleteMutationFn,
} from '@tanstack/react-db'
import {authSchema, userSchema,} from './schema'

import type { Auth, User } from './schema'

type CollectionKey = string | number

export const authCollection = createCollection<Auth>(
  localStorageCollectionOptions({
    storageKey: 'auth',
    getKey: (item: Auth) => item.key,
    onInsert: async () => true,
    onUpdate: async () => true,
    onDelete: async () => true,
    schema: authSchema,
  })
)

const headers = {
  Authorization: async () => {
    const auth = authCollection.get('current')

    return auth ? `Bearer ${auth.user_id}` : 'Unauthenticated'
  },
}

async function onError(error: Error) {
  const status =
    'status' in error && Number.isInteger(error.status)
    ? error.status as number
    : undefined

  if (status === 403 && authCollection.has('current')) {
    await authCollection.delete('current')

    return { headers }
  }

  if (status === 401) {
    await new Promise((resolve) => authCollection.subscribeChanges(resolve))

    return { headers }
  }

  throw error
}

const parser = {
  timestamp: (dateStr: string) => {
    // Timestamps sync in as naive datetime strings with no
    // timezone info because they're all implicitly UTC.
    const utcDateStr = dateStr.endsWith('Z') ? dateStr : `${dateStr}Z`
    const date: Date = new Date(utcDateStr)

    // Cast to `Value`` because we haven't fixed the typing yet
    // https://github.com/TanStack/db/pull/201
    return date as unknown as Value
  },
}

const baseShapeOptions = {
  headers,
  onError,
  parser,
}

function operationHandlers<Type extends object>() {
  return {
    onInsert: ingestMutations as InsertMutationFn<Type>,
    onUpdate: ingestMutations as UpdateMutationFn<Type>,
    onDelete: ingestMutations as DeleteMutationFn<Type>,
  }
}

function relativeUrl(path: string) {
  return `${window.location.origin}${path}`
}


// @ts-ignore
window.authCollection = authCollection
