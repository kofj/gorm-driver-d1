package gormd1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

type User struct {
	gorm.Model
	ID     int
	Name   string `gorm:"field:name"`
	Age    int    `gorm:"field:age"`
	Active bool
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

	ok = t.Run("HasTable", func(t *testing.T) {
		migrator := gdb.Migrator()
		if !migrator.HasTable(&User{}) {
			t.Errorf("expected table to exist")
		}
	})
	if !ok {
		return
	}

	var createdAt, updatedAt time.Time
	t.Run("Create", func(t *testing.T) {
		var user = &User{Name: "kofj", ID: 1, Age: 18, Active: true, Bin: bin1}
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
		var nuser = &User{ID: 1}
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
