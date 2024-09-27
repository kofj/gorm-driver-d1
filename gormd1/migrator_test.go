package gormd1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

type User struct {
	gorm.Model
	Name   string `gorm:"field:name;index:idx_name;unique"`
	Age    int    `gorm:"field:age"`
	Active bool
	Wallet float64
	Bin    []byte
}

var bin1 = []byte{0x99, 0x21, 0x33, 0x12, 0x48, 0x55}
var bin2 = []byte{0x33, 0x21, 0x33, 0x12, 0x48, 0x55}

func TestMigrator(t *testing.T) {

	var ok = t.Run("Migrate", func(t *testing.T) {
		var err = gdb.AutoMigrate(&User{})
		if err != nil {
			t.Errorf("failed to migrate: %v", err)
		}
	})
	if !ok {
		return
	}

	ok = t.Run("HasNoTable", func(t *testing.T) {
		migrator := gdb.Migrator()
		var result = migrator.HasTable("tab_not_exist")
		assert.Falsef(t, result, "expect table not exist.")
	})
	if !ok {
		return
	}

	ok = t.Run("HasTable", func(t *testing.T) {
		migrator := gdb.Migrator()
		if !migrator.HasTable(&User{}) {
			t.Errorf("expected table to exist")
		}
	})
	if !ok {
		return
	}

	t.Run("HasColumn", func(t *testing.T) {
		migrator := gdb.Migrator()
		var result = migrator.HasColumn(&User{}, "name")
		assert.Truef(t, result, "expect %v table exist name field.", &User{})
	})

	t.Run("HasIndex", func(t *testing.T) {
		migrator := gdb.Migrator()
		var result = migrator.HasIndex(&User{}, "idx_users_deleted_at")
		assert.Truef(t, result, "expect %v table exist index idx_users_deleted_at.", &User{})
		result = migrator.HasIndex(&User{}, "idx_name")
		assert.Truef(t, result, "expect %v table exist index idx_name.", &User{})
	})

	t.Run("RenameIndex", func(t *testing.T) {
		migrator := gdb.Migrator()
		var err = migrator.RenameIndex(&User{}, "idx_name", "idx_users_name")
		if err != nil {
			t.Errorf("failed to rename index: %v", err)
		}
		var result = migrator.HasIndex(&User{}, "idx_users_name")
		assert.Truef(t, result, "expect users table exist index idx_users_name.")
	})

	var createdAt, updatedAt time.Time
	t.Run("Create", func(t *testing.T) {
		var user = &User{Name: "kofj", Age: 18, Active: true, Bin: bin1, Wallet: 100.08}
		var err = gdb.Create(user).Error
		if err != nil {
			t.Errorf("failed to create record: %v", err)
		}
		createdAt = user.CreatedAt
		updatedAt = user.UpdatedAt
		assert.NotZero(t, user.CreatedAt, "created at")
		assert.NotZerof(t, user.UpdatedAt, "updated at")
	})

	t.Run("Find", func(t *testing.T) {
		var user User
		var err = gdb.Where("id", 1).First(&user).Error
		if err != nil {
			t.Errorf("failed to query record: %v", err)
			return
		}
		t.Logf("find user: %#+v", user)
		assert.Equalf(t, "kofj", user.Name, "user name")
		assert.Equalf(t, bin1, user.Bin, "user bin not equal")
		assert.Truef(t, user.Active, "active")
		assert.Equal(t, 100.08, user.Wallet)
		assert.Equalf(t, createdAt, user.CreatedAt, "CreatedAt not equal")
		assert.Equalf(t, updatedAt, user.UpdatedAt, "UpdatedAt not equal")
	})

	t.Run("Update", func(t *testing.T) {
		var tx = gdb.Model(&User{}).Where("id = ?", 1).Updates(
			map[string]interface{}{
				"name": "kofj1", "active": false, "bin": bin2,
			},
		)
		var err = tx.Error
		if err != nil {
			t.Errorf("failed to update record: %v", err)
		}
		if tx.RowsAffected != 1 {
			t.Errorf("expected RowsAffected to be 1; got %d", tx.RowsAffected)
		}
		var nuser = &User{}
		nuser.ID = 1
		gdb.First(nuser)
		assert.Equalf(t, "kofj1", nuser.Name, "user name")
		assert.Equalf(t, bin2, nuser.Bin, "user bin not equal")
		assert.Falsef(t, nuser.Active, "active")
		assert.Equalf(t, createdAt, nuser.CreatedAt, "CreatedAt not equal")
		assert.NotEqualf(t, updatedAt, nuser.UpdatedAt, "UpdatedAt should not equal")
	})

	t.Run("Delete", func(t *testing.T) {
		var tx = gdb.Delete(&User{}, 1)
		var err = tx.Error
		if err != nil {
			t.Errorf("failed to delete record: %v", err)
		}
		if tx.RowsAffected != 1 {
			t.Errorf("expected RowsAffected to be 1; got %d", tx.RowsAffected)
		}
	})

	t.Run("DropTable", func(t *testing.T) {
		migrator := gdb.Migrator()
		migrator.DropTable(&User{})
		if migrator.HasTable(&User{}) {
			t.Errorf("expected table to not exist")
		}
	})

}
